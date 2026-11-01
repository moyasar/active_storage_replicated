require "securerandom"

module ActiveStorageReplicated
  # One copy or delete that one endpoint still needs. Finished deletes stay for the retention period as
  # proof that a file was removed on purpose.
  #
  # A runner owns a task through a lease: `claimed_by` names the owner and `claimed_until` says how long the
  # claim holds. The runner renews the lease while it works and writes its result only if it still owns the
  # task. Request threads never replace a task under an active lease; they queue their change behind it.
  class Task < Record
    self.table_name = ActiveStorageReplicated.config.table_name

    enum :operation, { copy: 0, delete: 1, delete_prefixed: 2 }, prefix: true
    enum :state, { pending: 0, done: 1, skipped: 2 }

    validates :service_name, :target, :key, :run_after, presence: true

    scope :tombstones, -> { done.where(operation: [ :delete, :delete_prefixed ]) }
    # Leases use the database clock, so app hosts with different clocks agree on who holds a task.
    scope :leased, -> { where("#{quoted_table_name}.claimed_until > now()") }
    scope :not_leased, -> { where("#{quoted_table_name}.claimed_until IS NULL OR #{quoted_table_name}.claimed_until <= now()") }

    REQUEST_LEASE = 60

    # What `source` says about a task. A plain endpoint name means "copy from that endpoint".
    SOURCE_DEFERRED = "deferred"        # a delete held in memory during a database outage
    SOURCE_AUDIT = "audit"              # a delete queued by the audit
    SOURCE_RECONCILE = "reconcile"      # make this endpoint match the others: copy, or delete if they have none
    DIRECT_UPLOAD_PREFIX = "direct:"    # a copy from the endpoint a browser uploaded to
    AUDIT_COPY_PREFIX = "audit:"        # a copy from the endpoint the audit chose

    class << self
      # A task created with an owner is leased to it for a short time on the database clock, for work the
      # caller does right away.
      def enqueue!(service_name:, target:, operation:, key:, run_after: Time.current, source: nil, owner: nil,
                   created_at: nil, attempts: 0)
        attributes = {
          service_name: service_name.to_s,
          target: target.to_s,
          operation: operation,
          key: key,
          source: source&.to_s,
          run_after: run_after,
          attempts: attempts,
          claimed_by: owner
        }
        if created_at
          attributes[:created_at] = created_at
        end

        unless owner
          return create!(attributes)
        end

        transaction(requires_new: true) do
          task = create!(attributes)
          where(id: task.id).update_all(lease_sql(REQUEST_LEASE))
          task.claimed_until = Time.current + REQUEST_LEASE
          task.clear_changes_information
          task
        end
      end

      def enqueue_copies(service_name:, key:, targets:, run_after: Time.current, source: nil)
        targets.map do |target|
          enqueue!(service_name: service_name, target: target, operation: :copy, key: key, run_after: run_after, source: source)
        end
      end

      def new_owner
        SecureRandom.uuid
      end

      # Replaces pending tasks for one file before a request writes or deletes it. Returns a TakeOver with the
      # endpoints whose task a runner holds right now (the caller must not touch them and queues behind them),
      # and the replaced tasks, so a write that then fails can put them back.
      def take_over(service_name:, key:, targets:)
        held = Deferred.supersede(service_name: service_name, key: key, targets: targets)
        Deferred.wait_for_flush
        scope = pending.where(service_name: service_name.to_s, key: key, target: targets.map(&:to_s))
        take_over_scope(scope).tap { |take| take.held = held }
      rescue Exception
        if held.present?
          Deferred.add(held)
        end
        raise
      end

      # Same as take_over for every file under a prefix.
      def take_over_prefix(service_name:, prefix:)
        pattern = "#{sanitize_sql_like(prefix)}%"
        scope = pending.where(service_name: service_name.to_s).where("#{quoted_table_name}.key LIKE ?", pattern)
        Deferred.wait_for_flush
        take_over_scope(scope)
      end

      # Puts back tasks a take-over replaced, for a write that failed everywhere.
      def restore(take_over)
        take_over.replaced.each do |row|
          enqueue!(**row.slice(:service_name, :target, :operation, :key, :source, :created_at, :run_after, :attempts))
        end
      end

      # Endpoints with unfinished work for this key, by operation: { "replica" => "copy", ... }.
      def pending_operations(service_name:, key:)
        pending.where(service_name: service_name.to_s, key: key).order(:id).pluck(:target, :operation).to_h
      end

      # Due tasks of one kind for one endpoint, without an active lease and with no older pending task for the
      # same file and endpoint, so tasks for one file always run in the order they were recorded. Each lane
      # (service, endpoint, operation) is read in id order from its own index range.
      def claimable_lane(service_name:, target:, operation:, after_id: 0)
        claimable(after_id: after_id)
          .where(service_name: service_name.to_s, target: target.to_s, operation: operations.fetch(operation.to_s))
      end

      # Whether a lane has any due task, read from its run_after index, so a lane whose whole backlog is waiting
      # for a retry is skipped without reading the backlog.
      def lane_due?(service_name:, target:, operation:)
        pending
          .where(service_name: service_name.to_s, target: target.to_s, operation: operations.fetch(operation.to_s))
          .where(run_after: ..Time.current)
          .exists?
      end

      def claimable(after_id: 0, only: nil, skip_targets: [], skip_copies_to: [])
        older = from("#{quoted_table_name} older")
          .select(1)
          .where(older: { state: states[:pending] })
          .where("older.service_name = #{quoted_table_name}.service_name")
          .where("older.target = #{quoted_table_name}.target")
          .where("older.key = #{quoted_table_name}.key")
          .where("older.id < #{quoted_table_name}.id")

        scope = pending
          .not_leased
          .where(id: (after_id + 1)..)
          .where(run_after: ..Time.current)
          .where("NOT EXISTS (#{older.to_sql})")
          .order(:id)

        if only
          known = only.map { |service, targets| sanitize_sql([ "(service_name = ? AND target IN (?))", service.to_s, targets.map(&:to_s) ]) }
          scope = scope.where(known.any? ? known.join(" OR ") : "1 = 0")
        end

        if skip_targets.any?
          scope = scope.where.not(target: skip_targets.map(&:to_s))
        end

        skip_copies_to.each do |service, target|
          scope = scope.where.not(service_name: service.to_s, target: target.to_s, operation: operations[:copy])
        end

        scope
      end

      # Leases a batch to `owner` with SKIP LOCKED, so several processes can claim at once without overlap.
      # Lease changes leave indexed columns alone, so Postgres can update the rows in place. Pass `lane:` as
      # { service_name:, target:, operation: } to claim from one lane.
      def claim(owner:, after_id: 0, limit: ActiveStorageReplicated.config.batch_size, lane: nil, **filters)
        if lane && after_id.zero? && !lane_due?(**lane)
          return []
        end

        ids = transaction do
          scope = lane ? claimable_lane(after_id: after_id, **lane) : claimable(after_id: after_id, **filters)
          locked = scope.limit(limit).lock("FOR UPDATE SKIP LOCKED").pluck(:id)
          if locked.any?
            where(id: locked).update_all([ "claimed_by = ?, #{lease_sql(ActiveStorageReplicated.config.lease)}", owner ])
          end
          locked
        end

        if ids.empty?
          return []
        end

        where(id: ids).order(:id).to_a
      end

      # Gives back tasks this owner has not started.
      def release(ids, owner:)
        if ids.empty?
          return 0
        end

        owned(ids, owner).update_all(claimed_by: nil, claimed_until: nil)
      end

      # Extends the lease if the owner still holds it. Returns false when the task was replaced or taken.
      def renew(id, owner:, seconds: ActiveStorageReplicated.config.lease)
        owned(id, owner).leased.update_all(lease_sql(seconds)) == 1
      end

      def lease_sql(seconds)
        sanitize_sql([ "claimed_until = now() + make_interval(secs => ?)", seconds.to_f ])
      end

      def owned(ids, owner)
        where(id: ids, state: states[:pending], claimed_by: owner)
      end

      # Whether any work is pending for this key on these endpoints, counting pending prefix deletes that cover it.
      def pending_for?(service_name:, key:, targets:)
        scope = pending.where(service_name: service_name.to_s, target: targets.map(&:to_s))
        scope.where(key: key).exists? || covering_prefix_deletes(scope, key).any?(&:exists?)
      end

      # Endpoints with a pending prefix delete that covers this key.
      def pending_prefix_delete_targets(service_name:, key:)
        covering_prefix_deletes(pending.where(service_name: service_name.to_s), key)
          .flat_map { |relation| relation.distinct.pluck(:target) }
          .uniq
      end

      # Prefix deletes whose prefix starts this key, as two queries that each use an index: an exact match on the
      # key's folders (prefixes ending in "/", as Active Storage uses), and a scan of only the rare prefix deletes
      # that do not end in "/".
      def covering_prefix_deletes(scope, key)
        base = scope.operation_delete_prefixed

        [
          base.where(key: folders_of(key).presence || [ "" ]),
          base.where("#{quoted_table_name}.key NOT LIKE '%/'")
            .where("left(?, length(#{quoted_table_name}.key)) = #{quoted_table_name}.key", key)
        ]
      end

      def folders_of(key)
        key.enum_for(:scan, "/").map { Regexp.last_match.end(0) }.map { |length| key[0, length] }
      end

      # When the newest recorded delete of this key happened, counting prefix deletes that cover it.
      def last_deleted_at(service_name:, key:)
        scope = tombstones.where(service_name: service_name.to_s)

        times = [ scope.operation_delete.where(key: key).maximum(:created_at) ]
        times += covering_prefix_deletes(scope, key).map { |relation| relation.maximum(:created_at) }
        times.compact.max
      end

      def purge_finished(batch_size: 1_000, batches: 10)
        purged = 0
        cutoff = Time.current - ActiveStorageReplicated.config.retention.to_i

        batches.times do
          ids = where.not(state: :pending).where(updated_at: ...cutoff).limit(batch_size).pluck(:id)
          if ids.empty?
            break
          end

          purged += where(id: ids).delete_all
        end

        purged
      end

      def lease_end
        Time.current + ActiveStorageReplicated.config.lease.to_i
      end

      private

      # Rows a runner holds are reported busy; rows nobody holds are marked replaced. One cheap read in the
      # common case where nothing is pending.
      def take_over_scope(scope)
        if scope.none?
          return TakeOver.new([], [])
        end

        columns = [ :id, :target, :key, :operation, :source, :service_name, :created_at, :run_after, :attempts ]

        transaction(requires_new: true) do
          everything = scope.pluck(:id, :target, :key)
          free = scope.not_leased.lock("FOR UPDATE SKIP LOCKED").pluck(*columns)
          free_ids = free.map(&:first)
          scope.where(id: free_ids).update_all(
            state: states[:skipped], last_error: "superseded", claimed_by: nil, claimed_until: nil, updated_at: Time.current,
          )

          busy = everything.reject { |id, _target, _key| free_ids.include?(id) }.map { |_id, target, key| [ target, key ] }.uniq
          replaced = free.map do |_id, target, key, operation, source, service, created_at, run_after, attempts|
            {
              service_name: service, target: target, key: key, operation: operation, source: source,
              created_at: created_at, run_after: run_after, attempts: attempts
            }
          end

          TakeOver.new(busy, replaced)
        end
      end
    end

    # The methods below change the task only while `owner` still holds it, and report whether they did.

    # The result of a take-over: `busy` lists [target, key] pairs a runner holds; `replaced` lists the tasks
    # that were replaced, as attributes.
    TakeOver = Struct.new(:busy, :replaced, :given_back, :held) do
      def busy_targets
        busy.map(&:first).uniq
      end
    end

    # The endpoint named in `source`, without its prefix.
    def source_target
      source.to_s.delete_prefix(DIRECT_UPLOAD_PREFIX).delete_prefix(AUDIT_COPY_PREFIX).presence
    end

    def direct_upload?
      source.to_s.start_with?(DIRECT_UPLOAD_PREFIX)
    end

    def reconcile?
      source == SOURCE_RECONCILE
    end

    def finish!(owner: claimed_by)
      settle(owner, state: self.class.states[:done], last_error: nil)
    end

    def skip!(reason, owner: claimed_by)
      settle(owner, state: self.class.states[:skipped], last_error: reason)
    end

    def postpone!(seconds, reason = nil, owner: claimed_by)
      settle(owner, run_after: Time.current + seconds, last_error: reason || last_error)
    end

    def retry_later!(error, owner: claimed_by)
      next_attempt = attempts + 1
      delay = [ 30 * (2**[ next_attempt - 1, 10 ].min), ActiveStorageReplicated.config.max_backoff.to_i ].min

      changed = settle(
        owner,
        attempts: next_attempt,
        last_error: "#{error.class}: #{error.message}".truncate(2_000),
        run_after: Time.current + delay,
      )

      if changed && next_attempt == ActiveStorageReplicated.config.alert_after_attempts
        Instrumentation.instrument("task_stuck", task: self, error: error)
        Instrumentation.report(error, where: "task #{id} (#{operation} #{key} on #{target}) after #{next_attempt} attempts")
      end

      changed
    end

    private

    def settle(owner, **changes)
      scope = self.class.where(id: id, state: self.class.states[:pending])
      scope = owner ? scope.where(claimed_by: owner) : scope.where(claimed_by: nil)
      changed = scope.update_all(changes.merge(claimed_by: nil, claimed_until: nil, updated_at: Time.current)) == 1

      if changed
        assign_attributes(changes.except(:state).merge(claimed_by: nil, claimed_until: nil))
        if changes.key?(:state)
          self[:state] = self.class.states.key(changes[:state])
        end
        clear_changes_information
      end

      changed
    end
  end
end
