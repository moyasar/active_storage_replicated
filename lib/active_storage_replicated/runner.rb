require "digest"
require "concurrent"

module ActiveStorageReplicated
  # Runs pending copy and delete tasks. Several processes can run at once: each claims its own batch under a
  # lease that names it, renews the lease while it works, and records a result only while it still owns the task.
  # No database transaction is held while files move.
  class Runner
    class SourceUnavailable < StandardError; end
    class CopyMismatch < StandardError; end
    class LeaseLost < StandardError; end

    MULTIPART_THRESHOLD = 100 * 1024 * 1024
    MD5_ETAG = /\A[0-9a-f]{32}\z/
    LATE_UPLOAD_WINDOW = 24 * 60 * 60
    LATE_UPLOAD_RECHECK = 10 * 60

    # Endpoints whose ETags turned out not to be MD5 checksums, learned per process after several different
    # objects showed it, so one damaged object does not switch checks off for a whole endpoint.
    @etags_not_md5 = Concurrent::Set.new
    @etag_mismatches = Concurrent::Map.new
    ETAG_MISMATCHES_TO_LEARN = 3

    class << self
      attr_reader :etags_not_md5, :etag_mismatches

      def run_once(services = ActiveStorageReplicated.services)
        new(services).run
      end
    end

    def initialize(services)
      @services = services.index_by { |service| service.name.to_s }
      @owner = Task.new_owner
    end

    def run
      if @services.empty?
        return nil
      end

      summary = ActiveStorageReplicated.with_writing do
        process.merge(purged: Task.purge_finished)
      end
      Instrumentation.instrument("run", summary: summary)
      summary
    end

    private

    def config
      ActiveStorageReplicated.config
    end

    def health
      ActiveStorageReplicated.health
    end

    def process
      summary = { done: 0, skipped: 0, failed: 0, waiting: 0, busy: 0 }
      deadline = monotonic + config.time_budget.to_f

      lanes.each do |lane|
        last_id = 0

        while monotonic < deadline
          tasks = Task.claim(owner: @owner, after_id: last_id, lane: lane)
          if tasks.empty?
            break
          end

          stopped = false
          tasks.each_with_index do |task, index|
            if monotonic >= deadline
              Task.release(tasks[index..].map(&:id), owner: @owner)
              return summary
            end

            # An endpoint marked down during this run ends the lane at once, instead of claiming and giving
            # back the rest of its backlog one task at a time.
            if lane_blocked?(lane)
              Task.release(tasks[index..].map(&:id), owner: @owner)
              stopped = true
              break
            end

            last_id = task.id
            summary[perform(task)] += 1
          end

          if stopped
            break
          end
        end
      end

      summary
    end

    def lane_blocked?(lane)
      down = health.down_names
      if down.include?(lane[:target])
        return true
      end

      if lane[:operation] != "copy"
        return false
      end

      others = @services[lane[:service_name]].target_names - [ lane[:target] ]
      others.all? { |other| down.include?(other) }
    end

    # One lane per service, endpoint and operation that this process can act on: endpoints it knows and does
    # not consider down, and copies only where some other endpoint is reachable. Lanes start at a different
    # place on every run, so a long backlog in one lane does not starve the others.
    def lanes
      down = health.down_names
      all = @services.values.flat_map do |service|
        service.targets.flat_map do |target|
          if down.include?(target.name.to_s)
            next []
          end

          sources_up = (service.targets - [ target ]).any? { |other| !down.include?(other.name.to_s) }
          operations = sources_up ? Task.operations.keys : Task.operations.keys - [ "copy" ]
          operations.map { |operation| { service_name: service.name.to_s, target: target.name.to_s, operation: operation } }
        end
      end

      all.rotate(rand(all.size.nonzero? || 1))
    end

    def perform(task)
      service = @services[task.service_name]
      target = service&.target_named(task.target)

      if target.nil?
        task.postpone!(config.lease.to_i, "#{task.service_name}/#{task.target} is not configured in this process", owner: @owner)
        return :waiting
      end

      unless Task.renew(task.id, owner: @owner)
        return :busy
      end

      unless health.available?(target.name, trial: false)
        Task.release([ task.id ], owner: @owner)
        return :waiting
      end

      @wrote = false
      outcome = with_lease(task) { execute(service, target, task) }
      settle(task, outcome)
    rescue LeaseLost
      if @wrote
        correct_after_lost_lease(task)
      end
      :busy
    rescue SourceUnavailable, CopyMismatch, Errors::LocalFailure => e
      retry_task(task, e)
    rescue StandardError => e
      if Errors.database_error?(e)
        raise
      end

      health.failure(target.name, e)
      retry_task(task, e)
    end

    def settle(task, outcome)
      case outcome
      in :done
        if task.finish!(owner: @owner)
          :done
        else
          correct_after_lost_lease(task)
          :busy
        end
      in [ :skipped, reason ]
        task.skip!(reason, owner: @owner) ? :skipped : :busy
      in [ :postponed, seconds, reason ]
        task.postpone!(seconds, reason, owner: @owner)
        :waiting
      in :no_source
        task.postpone!(config.cool_off.to_i, "no other endpoint is reachable", owner: @owner)
        :waiting
      end
    end

    # The write may have reached the endpoint after the task was replaced or taken, so the endpoint may hold
    # something the newer intent did not want. Queue a task that makes it match the other endpoints that are
    # up to date: a copy of their file, or a delete if all of them answer that they do not have it. A service
    # with one endpoint has no other endpoint to match.
    def correct_after_lost_lease(task)
      if task.operation_delete_prefixed? || @services[task.service_name].targets.one?
        return
      end

      Task.enqueue!(
        service_name: task.service_name, target: task.target, operation: :copy, key: task.key, source: Task::SOURCE_RECONCILE,
      )
    rescue ActiveRecord::ActiveRecordError => e
      Instrumentation.report(e, where: "correction for task #{task.id}")
    end

    def retry_task(task, error)
      task.retry_later!(error, owner: @owner)
      ActiveStorageReplicated.logger.warn(
        "[ActiveStorageReplicated] task #{task.id} (#{task.operation} #{task.key} on #{task.target}) failed: " \
        "#{error.class}: #{error.message}",
      )
      :failed
    end

    # Keeps the lease alive while the work runs.
    def with_lease(task)
      lost = false
      stop = Queue.new
      renewer = Thread.new do
        loop do
          if stop.pop(timeout: [ config.lease.to_f / 3, 1 ].max)
            break
          end

          renewed = begin
            ActiveStorageReplicated.with_writing do
              Task.connection_pool.with_connection { Task.renew(task.id, owner: @owner) }
            end
          rescue StandardError
            nil
          end

          # Only a renewal the database refused proves the lease is gone. A renewal that failed with an error
          # proves nothing; the result is still written only if the task is ours at the end.
          if renewed == false
            lost = true
            break
          end
        end
      end

      result = yield
      if lost
        raise LeaseLost
      end

      result
    ensure
      stop&.push(true)
      renewer&.join
    end

    def execute(service, target, task)
      if task.operation_copy?
        return copy(service, target, task)
      end

      if task.operation_delete? && [ Task::SOURCE_DEFERRED, Task::SOURCE_AUDIT ].include?(task.source)
        written = content_written_at(target, task.key)
        if written && written > task.created_at + Audit::CLOCK_MARGIN
          return [ :skipped, "the file was written after this delete was asked for" ]
        end
      end

      confirm_lease(task)
      @wrote = true
      if task.operation_delete_prefixed?
        delete_prefixed_written_before(target, task.key, task.created_at)
      else
        target.delete(task.key)
      end
      health.success(target.name)
      :done
    end

    # A prefix delete that ran late removes only files clearly written before it was asked for. Anything written
    # within the clock margin is kept: file times can be whole seconds and clocks differ, and a stray file is
    # better than a lost one.
    def delete_prefixed_written_before(target, prefix, asked_at)
      cutoff = asked_at - Audit::CLOCK_MARGIN
      if s3?(target)
        target.bucket.objects(prefix: prefix).each do |summary|
          written = summary.last_modified
          if written > cutoff
            written = written_at_of(summary.object.tap(&:load))
          end

          if written <= cutoff
            summary.object.delete
          end
        end
      elsif target.respond_to?(:root)
        root = Pathname.new(target.root)
        Dir.glob("**/*", base: root.to_s).each do |relative|
          path = root.join(relative)
          key = relative.split("/", 3).last
          if path.file? && key.start_with?(prefix) && path.mtime <= cutoff
            path.delete
          end
        end
      else
        target.delete_prefixed(prefix)
      end
    end

    # The last check before bytes leave this process: the task must still be ours.
    def confirm_lease(task)
      unless Task.renew(task.id, owner: @owner)
        raise LeaseLost
      end
    end

    # When the endpoint's copy of the file was first written, or nil if it has none.
    def content_written_at(target, key)
      if s3?(target)
        object = target.bucket.object(key)
        object.load
        written_at_of(object)
      elsif target.exist?(key) && target.respond_to?(:path_for)
        File.mtime(target.path_for(key))
      end
    rescue ActiveStorage::FileNotFoundError
      nil
    rescue StandardError => e
      if Errors.http_status(e) == 404
        return nil
      end

      raise
    end

    # Copies from another endpoint that has the current file: endpoints that are themselves waiting for a copy
    # or a delete of this key are never used as a source. Keeps content type, disposition and metadata, and
    # checks the bytes against the source checksum when the source offers a real one.
    def copy(service, target, task)
      if task.reconcile?
        return settle_key(service, target, task)
      end

      sources = sources_for(service, target, task)
      if sources.empty?
        return settle_key(service, target, task)
      end

      if sources.none? { |source| health.available?(source.name, trial: false) }
        return :no_source
      end

      copy_from(sources, target, task) || not_found_outcome(task)
    end

    # Downloads from the first source that has the file and writes it to the target. Nil when no source has it.
    def copy_from(sources, target, task)
      with_temporary_file(task.key) do |file|
        source, attributes = download_from_any(sources, task.key, file)
        if source.nil?
          return nil
        end

        md5 = verified_md5(source, task.key, file, attributes)
        file.rewind
        confirm_lease(task)
        @wrote = true
        write_copy(target, task.key, file, attributes, md5.base64digest)
        health.success(target.name)
        :done
      end
    end

    # Settles one endpoint for one key by what every endpoint holds now, for cases where no task history can
    # say which endpoint is right: corrections after a lost lease, interrupted writes, and endpoints waiting on
    # each other. Every endpoint must answer; endpoints with a pending delete count as not holding the file.
    # - A delete asked for clearly after the newest content: the endpoint's copy is deleted.
    # - Another endpoint holds newer content: it is copied here.
    # - This endpoint holds the newest content: nothing to do here, and endpoints without it get a copy.
    # A file is never deleted without a recorded or pending delete as proof.
    def settle_key(service, target, task)
      deleting = Task.pending.where(service_name: task.service_name, key: task.key)
        .where(operation: [ Task.operations[:delete], Task.operations[:delete_prefixed] ]).where.not(id: task.id)
      delete_asked_at = [
        Task.last_deleted_at(service_name: task.service_name, key: task.key),
        deleting.where(key: task.key).maximum(:created_at),
        *Task.covering_prefix_deletes(Task.pending.where(service_name: task.service_name), task.key).map { |relation| relation.maximum(:created_at) }
      ].compact.max
      deleting_targets = deleting.where(key: task.key).distinct.pluck(:target) +
        Task.pending_prefix_delete_targets(service_name: task.service_name, key: task.key)

      written = service.targets.to_h { |endpoint| [ endpoint, written_at_or_unavailable(endpoint, task.key, target) ] }
      holders = written.reject { |endpoint, time| time.nil? || deleting_targets.include?(endpoint.name.to_s) }
      newest_time = holders.values.max
      newest = service.targets.find { |endpoint| holders.key?(endpoint) && holders[endpoint] == newest_time }

      if delete_asked_at && (newest_time.nil? || delete_asked_at > newest_time + Audit::CLOCK_MARGIN)
        if written[target]
          confirm_lease(task)
          @wrote = true
          target.delete(task.key)
          health.success(target.name)
        end

        return :done
      end

      if newest.nil?
        return [ :skipped, "no endpoint holds the file and no delete was asked for" ]
      end

      if newest != target && (written[target].nil? || written[target] < newest_time)
        return copy_from([ newest ], target, task) || raise(SourceUnavailable, "#{newest.name} lost the file while settling")
      end

      copy_to_endpoints_without(service, target, task, holders)
      :done
    end

    def written_at_or_unavailable(endpoint, key, target)
      unless endpoint == target || health.available?(endpoint.name, trial: false)
        raise SourceUnavailable, "#{endpoint.name} is down, so #{key} cannot be settled yet"
      end

      content_written_at(endpoint, key)
    rescue SourceUnavailable
      raise
    rescue StandardError => e
      if endpoint == target
        raise
      end

      raise SourceUnavailable, "#{endpoint.name}: #{e.class}: #{e.message}"
    end

    # This endpoint holds the newest content; queue copies from it to endpoints that lack the file, unless they
    # already have work queued for this key.
    def copy_to_endpoints_without(service, target, task, holders)
      busy = Task.pending.where(service_name: task.service_name, key: task.key).where.not(id: task.id).distinct.pluck(:target)

      (service.targets - [ target ]).each do |endpoint|
        if holders.key?(endpoint) || busy.include?(endpoint.name.to_s)
          next
        end

        Task.enqueue!(service_name: task.service_name, target: endpoint.name, operation: :copy, key: task.key, source: target.name)
      end
    end

    def with_temporary_file(key)
      file = begin
        Tempfile.create([ "active-storage-replicated", File.extname(key) ], binmode: true)
      rescue SystemCallError => e
        raise Errors::LocalFailure, "could not create a temporary file: #{e.class}: #{e.message}"
      end

      yield file
    ensure
      if file
        file.close
        File.unlink(file.path) if File.exist?(file.path)
      end
    end

    # Endpoints that hold the current file: a named source if there is one, otherwise the others without
    # pending work for this key. An endpoint that is itself waiting for a copy or a delete is never trusted;
    # when every other endpoint is waiting, the copy waits too, and is reported if that lasts.
    def sources_for(service, target, task)
      named = service.targets.find { |source| source.name.to_s == task.source_target && source != target }
      if named
        return [ named ]
      end

      others = service.targets - [ target ]
      waiting = Task.pending.where(service_name: task.service_name, key: task.key).where.not(id: task.id).distinct.pluck(:target)
      waiting += Task.pending_prefix_delete_targets(service_name: task.service_name, key: task.key)
      others.reject { |source| waiting.include?(source.name.to_s) }
    end

    # Every source answered that it does not have the file. A copy for a direct upload waits up to a day for a
    # late upload; any other copy is skipped, because the file was deleted.
    def not_found_outcome(task)
      if task.direct_upload? && task.created_at > Time.current - LATE_UPLOAD_WINDOW
        return [ :postponed, LATE_UPLOAD_RECHECK, "waiting for the file on #{task.source_target}" ]
      end

      [ :skipped, "the file is not on any other target" ]
    end

    # Asks each source only right before using it. Raises when a source could not be asked, so the copy is
    # retried instead of being skipped while an endpoint holding the file is down.
    def download_from_any(sources, key, file)
      unknown = nil

      sources.each do |source|
        unless health.available?(source.name, trial: false)
          unknown ||= SourceUnavailable.new("#{source.name} is down")
          next
        end

        file.truncate(0)
        file.rewind
        source.download(key) { |chunk| write_local(file, chunk) }
        file.flush
        health.success(source.name)
        return [ source, attributes_of(source, key) ]
      rescue ActiveStorage::FileNotFoundError
        health.answered(source.name)
      rescue Errors::LocalFailure
        raise
      rescue StandardError => e
        health.failure(source.name, e)
        unknown = SourceUnavailable.new("#{source.name}: #{e.class}: #{e.message}")
      end

      if unknown
        raise unknown
      end

      [ nil, nil ]
    end

    def note_non_md5_etag(source, key)
      keys = self.class.etag_mismatches.compute_if_absent(source.name.to_s) { Concurrent::Set.new }
      keys << key
      Instrumentation.report(
        RuntimeError.new("#{source.name}/#{key}: bytes are stable but do not match the ETag"), where: "copy checksum",
      )

      if keys.size >= ETAG_MISMATCHES_TO_LEARN
        self.class.etags_not_md5 << source.name.to_s
        ActiveStorageReplicated.logger.warn("[ActiveStorageReplicated] #{source.name} returns ETags that are not MD5 checksums")
      end
    end

    def write_local(file, chunk)
      file.write(chunk)
    rescue SystemCallError, IOError => e
      raise Errors::LocalFailure, "could not write a temporary file: #{e.class}: #{e.message}"
    end

    # Some S3-compatible stores return 32-hex ETags that are not MD5s. A mismatch is confirmed by downloading
    # again: if the bytes are stable, the endpoint's ETags are not checksums, and that is remembered.
    def verified_md5(source, key, file, attributes)
      md5 = Digest::MD5.file(file.path)
      expected = attributes[:md5_etag]
      if expected.nil? || expected == md5.hexdigest || self.class.etags_not_md5.include?(source.name.to_s)
        return md5
      end

      again = Digest::MD5.new
      begin
        source.download(key) { |chunk| again.update(chunk) }
      rescue StandardError => e
        raise SourceUnavailable, "#{source.name}: #{e.class}: #{e.message}"
      end

      if again.hexdigest == md5.hexdigest
        note_non_md5_etag(source, key)
        return md5
      end

      raise CopyMismatch, "copy of #{key} from #{source.name} does not match its checksum"
    end

    def attributes_of(source, key)
      unless s3?(source)
        written = content_written_at(source, key)
        return written ? { written_at: written } : {}
      end

      object = source.bucket.object(key)
      object.load
      {
        content_type: object.content_type,
        content_disposition: object.content_disposition,
        metadata: object.metadata,
        md5_etag: md5_etag(object),
        written_at: written_at_of(object)
      }.compact
    end

    def written_at_of(object)
      stamp = object.metadata[Audit::WRITTEN_AT_METADATA]
      stamp ? Time.iso8601(stamp) : object.last_modified
    end

    # An ETag is the MD5 of the bytes only for single-part objects that are unencrypted or encrypted with
    # S3-managed keys. Anything else is not comparable.
    def md5_etag(object)
      etag = object.etag.to_s.delete('"')
      if !etag.match?(MD5_ETAG) || object.sse_customer_algorithm.present?
        return nil
      end

      if object.server_side_encryption.present? && object.server_side_encryption != "AES256"
        return nil
      end

      etag
    end

    # The copy carries the time its content was first written, so the audit can tell an older version that
    # was copied late from a newer one.
    def write_copy(target, key, file, attributes, md5)
      unless s3?(target)
        target.upload(key, file, checksum: md5, content_type: attributes[:content_type])
        keep_content_time_on_disk(target, key, attributes[:written_at])
        return
      end

      metadata = (attributes[:metadata] || {}).dup
      if attributes[:written_at]
        metadata[Audit::WRITTEN_AT_METADATA] ||= attributes[:written_at].utc.iso8601(3)
      end

      options = target.upload_options.merge(
        content_type: attributes[:content_type],
        content_disposition: attributes[:content_disposition],
        metadata: metadata.presence,
      ).compact

      if file.size < MULTIPART_THRESHOLD
        target.bucket.object(key).put(body: file, content_md5: md5, **options)
      elsif defined?(Aws::S3::TransferManager)
        Aws::S3::TransferManager.new(client: target.client.client)
          .upload_file(file.path, bucket: target.bucket.name, key: key, **options)
      else
        target.bucket.object(key).upload_file(file.path, **options)
      end
    end

    def s3?(target)
      defined?(ActiveStorage::Service::S3Service) && target.is_a?(ActiveStorage::Service::S3Service)
    end

    # A local-folder endpoint keeps no metadata, so its copy keeps the source's content time as its file time.
    def keep_content_time_on_disk(target, key, written_at)
      if written_at && target.respond_to?(:path_for)
        File.utime(written_at, written_at, target.path_for(key))
      end
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
