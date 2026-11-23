require "active_storage/service"
require "tempfile"
require "concurrent"
require "active_storage_replicated"

module ActiveStorage
  # Writes to healthy endpoints, reads from an endpoint that is known to have the current file, and records in
  # the to-do table whatever an endpoint missed.
  class Service::ReplicatedService < Service
    class StreamInterrupted < StandardError; end
    class EndpointsBusy < ActiveStorage::Error; end

    WRITERS = Concurrent::ThreadPoolExecutor.new(
      name: "active-storage-replicated-writers",
      min_threads: 0,
      max_threads: 8,
      max_queue: 8,
      fallback_policy: :caller_runs,
    )

    attr_reader :targets

    def self.build(targets:, name:, configurator:, public: false, **)
      new(
        targets: targets.map { |target_name| configurator.build(target_name) },
        public: public,
      ).tap { |service| service.name = name }
    end

    def initialize(targets:, public: false)
      if targets.size < 2
        raise ArgumentError, "A replicated service needs at least two targets"
      end

      @targets = targets
      @public = public
    end

    def health
      ActiveStorageReplicated.health
    end

    def target_named(target_name)
      targets.find { |target| target.name.to_s == target_name.to_s }
    end

    def target_names
      targets.map { |target| target.name.to_s }
    end

    # The first endpoint that is not marked down. Never spends a trial call, because callers of this method
    # (links, direct uploads) cannot fall back to another endpoint once they have picked one.
    def first_available_target(among = targets)
      among.find { |target| !health.down?(target.name) } || among.first
    end

    # Writes to endpoints in order until `sync_copies` of them have the file, then returns. With
    # `sync_copies = :all` and `parallel_writes`, the endpoints are written at the same time. Endpoints not
    # written get a copy task. An endpoint whose task a runner is working on right now is never written
    # directly; its copy is queued behind that work. Fails only if no other endpoint accepted the file.
    def upload(key, io, checksum: nil, **options)
      with_replayable(io) do |replayable|
        take = take_over(key, "upload")
        writable = targets.reject { |target| take.busy_targets.include?(target.name.to_s) }
        if writable.empty?
          give_back(take, key)
          raise EndpointsBusy, "Every endpoint is busy with #{key}; try again shortly"
        end

        begin
          written, last_error = write_to(writable, key, replayable, checksum: checksum, **options)
        rescue Exception # rubocop:disable Lint/RescueException -- also a request timeout raised into this thread
          give_back(take, key)
          settle_later(key)
          raise
        end

        if written.empty?
          give_back(take, key)
          raise(last_error || ActiveStorage::Error.new("No storage target accepted #{key}"))
        end

        track_after_write(key, written)
      end
    end

    # Streams a file that is generated on the fly. A stream cannot be replayed, so it goes to one endpoint; if
    # that endpoint fails before the first byte is produced, the next one is tried. The others get a copy task.
    def stream_upload(key, content_type: nil, &block)
      unless block
        raise ArgumentError, "A block is required"
      end

      take = take_over(key, "stream_upload")
      candidates = targets.reject { |target| take.busy_targets.include?(target.name.to_s) }
      if candidates.empty?
        give_back(take, key)
        raise EndpointsBusy, "Every endpoint is busy with #{key}; try again shortly"
      end

      written = begin
        stream_to(candidates, key, content_type, take, &block)
      rescue Exception => e # rubocop:disable Lint/RescueException -- also a request timeout raised into this thread
        give_back(take, key)
        unless e.is_a?(StandardError)
          settle_later(key)
        end
        raise
      end

      track_after_write(key, [ written ])
      key
    end

    def stream_to(candidates, key, content_type, take, &block)
      last_error = nil
      each_target_to_try(candidates) do |target|
        attempt = StreamAttempt.new(block)

        begin
          write_stream(target, key, content_type: content_type, attempt: attempt)
        rescue StandardError => e
          if attempt.caller_error
            give_back(take, key)
            raise attempt.caller_error
          end

          target_failed(target, :stream_upload, key, e)
          last_error = e

          if attempt.started?
            give_back(take, key)
            raise
          end

          next
        end

        health.success(target.name)
        return target
      end

      give_back(take, key)
      raise(last_error || ActiveStorage::Error.new("No storage target accepted #{key}"))
    end
    private :stream_to

    # Records copies for the endpoints a successful write missed. If the thread is interrupted (a request
    # timeout) before that is recorded, the copies are kept in memory instead, and the write stands.
    def track_after_write(key, written)
      track_upload(key, written: written)
    rescue Exception # rubocop:disable Lint/RescueException -- also a request timeout raised into this thread
      missing = (targets - written).map { |target| target.name.to_s }
      ActiveStorageReplicated::Deferred.add(
        missing.map { |target| ActiveStorageReplicated::Deferred.row(service_name: name, target: target, operation: :copy, key: key) },
      )
      raise
    end
    private :track_after_write

    # Queues copies for the endpoints that did not get the file. Pass `take_over: true` when the caller did not
    # already replace pending tasks for this key before writing.
    def track_upload(key, written:, take_over: false)
      missing = (targets - written).map { |target| target.name.to_s }
      if missing.empty? && !take_over
        return nil
      end

      recorded = bookkeeping("track_upload #{key}") do
        if take_over
          ActiveStorageReplicated::Task.take_over(service_name: name, key: key, targets: written.map { |target| target.name.to_s })
        end

        ActiveStorageReplicated::Task.enqueue_copies(service_name: name, key: key, targets: missing)
      end

      if recorded.nil? && missing.any?
        ActiveStorageReplicated::Deferred.add(
          missing.map { |target| ActiveStorageReplicated::Deferred.row(service_name: name, target: target, operation: :copy, key: key) },
        )
      end

      nil
    end

    def delete(key)
      replicate_delete(:delete, key)
    end

    def delete_prefixed(prefix)
      replicate_delete(:delete_prefixed, prefix)
    end

    def delete_many(keys)
      keys.each { |key| delete(key) }
      nil
    end

    def download(key, &block)
      if block
        read(key) { |target| stream_from(target, key, &block) }
      else
        read(key) { |target| target.download(key) }
      end
    end

    def download_chunk(key, range)
      read(key) { |target| target.download_chunk(key, range) }
    end

    def exist?(key)
      read(key) do |target|
        if target.exist?(key)
          true
        else
          raise ActiveStorage::FileNotFoundError
        end
      end
    rescue ActiveStorage::FileNotFoundError
      false
    end

    def url(key, **options)
      url_target(key).url(key, **options)
    end

    def checked_url(key, **options)
      holder = read(key) do |target|
        unless target.exist?(key)
          raise ActiveStorage::FileNotFoundError.new(key)
        end

        target
      end

      holder.url(key, **options)
    end

    def redirect_url(key, expires_in:, **options)
      ActiveStorageReplicated::Redirect.url(key, expires_in: expires_in, service: self, **options)
    end

    # The browser uploads straight to one endpoint. Copies to the others are queued now, from that endpoint,
    # to run once the signed link has expired. A copy waits for a late upload for up to a day.
    def url_for_direct_upload(key, expires_in:, **options)
      target = first_available_target
      others = target_names - [ target.name.to_s ]
      run_after = Time.current + expires_in.to_i + ActiveStorageReplicated.config.direct_upload_grace.to_i

      recorded = bookkeeping("url_for_direct_upload #{key}") do
        ActiveStorageReplicated::Task.enqueue_copies(
          service_name: name, key: key, targets: others, run_after: run_after,
          source: "#{ActiveStorageReplicated::Task::DIRECT_UPLOAD_PREFIX}#{target.name}",
        )
      end

      if recorded.nil?
        ActiveStorageReplicated::Deferred.add(others.map do |other|
          ActiveStorageReplicated::Deferred.row(
            service_name: name, target: other, operation: :copy, key: key, run_after: run_after,
            source: "#{ActiveStorageReplicated::Task::DIRECT_UPLOAD_PREFIX}#{target.name}",
          )
        end)
      end

      target.url_for_direct_upload(key, expires_in: expires_in, **options)
    end

    def headers_for_direct_upload(key, **options)
      first_available_target.headers_for_direct_upload(key, **options)
    end

    def compose(source_keys, destination_key, **options)
      take = take_over(destination_key, "compose")
      candidates = targets.reject { |target| take.busy_targets.include?(target.name.to_s) }
      last_error = nil

      each_target_to_try(candidates) do |target|
        target.compose(source_keys, destination_key, **options)
        health.success(target.name)
        track_after_write(destination_key, [ target ])
        return nil
      rescue StandardError => e
        target_failed(target, :compose, destination_key, e)
        last_error = e
      end

      give_back(take, destination_key)
      raise(last_error || ActiveStorage::Error.new("No storage target could compose #{destination_key}"))
    rescue Exception => e # rubocop:disable Lint/RescueException -- also a request timeout raised into this thread
      unless e.is_a?(StandardError)
        give_back(take, destination_key)
        settle_later(destination_key)
      end
      raise
    end

    def update_metadata(key, **metadata)
      targets.each do |target|
        target.update_metadata(key, **metadata)
      rescue StandardError => e
        target_failed(target, :update_metadata, key, e)
      end
    end

    private

    # Tracks whether the caller's block has started and whether it raised its own error, so a failure before
    # any byte can move to the next endpoint and the caller always gets back its own exception.
    class StreamAttempt
      PIPE_ERRORS = [ IOError, Errno::EPIPE ].freeze

      attr_reader :caller_error

      def initialize(block)
        @block = block
        @started = false
        @caller_error = nil
      end

      def started?
        @started
      end

      def call(io)
        @started = true
        @block.call(io)
      rescue *PIPE_ERRORS
        raise
      rescue StandardError => e
        @caller_error = e
        raise
      end

      def to_proc
        method(:call).to_proc
      end
    end

    # Yields an IO that can be read more than once. A stream that cannot be rewound is copied to a temporary
    # file first, so every endpoint receives the whole file.
    def with_replayable(io)
      if replayable?(io)
        return yield(io)
      end

      Tempfile.create("active-storage-replicated", binmode: true) do |file|
        IO.copy_stream(io, file)
        file.flush
        file.rewind
        yield file
      end
    end

    def write_to(candidates, key, io, checksum:, **options)
      wanted = ActiveStorageReplicated.config.sync_copies

      if wanted == :all && ActiveStorageReplicated.config.parallel_writes && candidates.size > 1 && reopenable?(io)
        write_in_parallel(ordered_for_writing(candidates), key, io, checksum: checksum, **options)
      else
        write_in_order(candidates, key, io, wanted, checksum: checksum, **options)
      end
    end

    def write_in_order(candidates, key, io, wanted, checksum:, **options)
      written = []
      last_error = nil

      each_target_to_try(candidates) do |target|
        if wanted != :all && written.size >= wanted
          break
        end

        rewind(io)

        begin
          target.upload(key, io, checksum: checksum, **options)
          health.success(target.name)
          written << target
        rescue ActiveStorage::IntegrityError
          raise
        rescue StandardError => e
          last_error = e
          target_failed(target, :upload, key, e)
        end
      end

      [ written, last_error ]
    end

    def write_in_parallel(ordered, key, io, checksum:, **options)
      io.flush if io.respond_to?(:flush)

      futures = ordered.map do |target|
        Concurrent::Promises.future_on(WRITERS, target) do |endpoint|
          with_own_io(io) { |own| endpoint.upload(key, own, checksum: checksum, **options) }
          [ endpoint, nil ]
        rescue StandardError => e
          [ endpoint, e ]
        end
      end

      written = []
      last_error = nil
      integrity_error = nil

      futures.map(&:value!).each do |target, error|
        if error.nil?
          health.success(target.name)
          written << target
        elsif error.is_a?(ActiveStorage::IntegrityError)
          integrity_error = error
        else
          last_error = error
          target_failed(target, :upload, key, error)
        end
      end

      if integrity_error
        raise integrity_error
      end

      [ written, last_error ]
    end

    # Pipes and sockets answer to `rewind` but cannot actually go back.
    def replayable?(io)
      if io.is_a?(StringIO) || reopenable?(io)
        return true
      end

      if io.is_a?(IO)
        return io.stat.file?
      end

      io.respond_to?(:rewind)
    end

    # Only objects that are the file itself can be reopened by path: a File, a Tempfile, or an uploaded file
    # wrapping a Tempfile. Wrappers that merely expose a path (a gzip reader, a CSV) yield other bytes.
    def reopenable?(io)
      if io.is_a?(StringIO)
        return true
      end

      !file_path_of(io).nil?
    end

    def file_path_of(io)
      file = io.respond_to?(:tempfile) ? io.tempfile : io
      if file.is_a?(File) || file.is_a?(Tempfile)
        path = file.path
        return path if path.is_a?(String) && File.file?(path)
      end

      nil
    end

    def with_own_io(io)
      if io.is_a?(StringIO)
        yield StringIO.new(io.string)
      else
        File.open(file_path_of(io), "rb") { |file| yield file }
      end
    end

    def rewind(io)
      if io.respond_to?(:rewind)
        io.rewind
      end
    end

    # Endpoints that may be called now, in configured order, with endpoints in their cool-off left out.
    # When every endpoint is down, all of them are returned so the call still has a chance.
    def ordered_for_writing(candidates)
      available = candidates.select { |target| health.available?(target.name) }
      available.empty? ? candidates : available
    end

    # Yields endpoints in order, asking the breaker only right before each one is used.
    def each_target_to_try(candidates, &block)
      tried = false

      candidates.each do |target|
        if health.available?(target.name)
          tried = true
          yield target
        end
      end

      unless tried
        candidates.each(&block)
      end
    end

    def write_stream(target, key, content_type:, attempt:)
      if s3?(target)
        options = target.upload_options.merge(content_type: content_type).compact

        if defined?(Aws::S3::TransferManager)
          Aws::S3::TransferManager.new(client: target.client.client)
            .upload_stream(bucket: target.bucket.name, key: key, thread_count: 2, **options, &attempt)
        else
          target.bucket.object(key).upload_stream(thread_count: 2, **options, &attempt)
        end
      else
        Tempfile.create([ "active-storage-replicated", File.extname(key) ], binmode: true) do |file|
          attempt.call(file)
          file.flush
          file.rewind
          target.upload(key, file, content_type: content_type)
        end
      end
    end

    def s3?(target)
      defined?(ActiveStorage::Service::S3Service) && target.is_a?(ActiveStorage::Service::S3Service)
    end

    # Records a task per endpoint before deleting, so a crash halfway never loses a delete, and the finished
    # tasks prove the delete later. Endpoints a runner is working on get a delete queued behind that work.
    def replicate_delete(operation, key)
      owner = ActiveStorageReplicated::Task.new_owner
      take = bookkeeping("#{operation} #{key}", default: nil) do
        if operation == :delete_prefixed
          ActiveStorageReplicated::Task.take_over_prefix(service_name: name, prefix: key)
        else
          ActiveStorageReplicated::Task.take_over(service_name: name, key: key, targets: target_names)
        end
      end
      busy = take ? take.busy : []
      busy_targets = busy.map(&:first).uniq

      plan = targets.map do |target|
        [ target, !busy_targets.include?(target.name.to_s) && health.available?(target.name) ]
      end

      tasks = bookkeeping("#{operation} #{key}", default: nil) do
        recorded = plan.to_h do |target, now|
          task = ActiveStorageReplicated::Task.enqueue!(
            service_name: name, target: target.name, operation: operation, key: key, owner: now ? owner : nil,
          )
          [ target, task ]
        end

        busy.each do |target, busy_key|
          ActiveStorageReplicated::Task.enqueue!(service_name: name, target: target, operation: :delete, key: busy_key)
        end

        recorded
      end

      deleted = []

      plan.each do |target, now|
        unless now
          next
        end

        task = tasks&.fetch(target)
        if task && !bookkeeping("#{operation} #{key}", default: true) { ActiveStorageReplicated::Task.renew(task.id, owner: owner, seconds: ActiveStorageReplicated::Task::REQUEST_LEASE) }
          next
        end

        begin
          target.public_send(operation, key)
          health.success(target.name)
          deleted << target
        rescue StandardError => e
          target_failed(target, operation, key, e)
          record_delete_outcome(tasks, target, operation, key, error: e)
          next
        end

        record_delete_outcome(tasks, target, operation, key)
      end

      if tasks.nil?
        defer_delete(deleted, busy, operation, key)
      end

      nil
    end

    def record_delete_outcome(tasks, target, operation, key, error: nil)
      task = tasks&.fetch(target)
      if task.nil?
        return
      end

      bookkeeping("#{operation} #{key}") do
        error ? task.retry_later!(error) : task.finish!
      end
    end

    # The database was unavailable: keep the delete records in memory. Deletes that succeeded become finished
    # records (the proof the audit needs); the others, including failed ones, stay pending.
    def defer_delete(deleted, busy, operation, key)
      rows = targets.map do |target|
        ActiveStorageReplicated::Deferred.row(
          service_name: name, target: target.name, operation: operation, key: key,
          state: deleted.include?(target) ? :done : :pending,
        )
      end
      rows += busy.map do |target, busy_key|
        ActiveStorageReplicated::Deferred.row(service_name: name, target: target, operation: :delete, key: busy_key)
      end

      ActiveStorageReplicated::Deferred.add(rows)
    end

    # Endpoints that should have the current file: not those waiting for a copy (missing or older version),
    # and not those waiting for a delete (the file was deleted).
    def fresh_targets(key, context)
      pending = bookkeeping("#{context} #{key}", default: {}) do
        operations = ActiveStorageReplicated::Task.pending_operations(service_name: name, key: key)
        ActiveStorageReplicated::Task.pending_prefix_delete_targets(service_name: name, key: key).each do |target|
          operations[target] = "delete_prefixed"
        end
        operations
      end

      fresh = targets.reject { |target| pending.key?(target.name.to_s) }
      if fresh.any?
        return fresh
      end

      if pending.value?("delete") || pending.value?("delete_prefixed")
        return []
      end

      targets
    end

    # A "not found" is final only when no endpoint before it failed, unless `read_fallback_on_not_found` asks
    # every endpoint before giving up.
    def read(key)
      candidates = fresh_targets(key, "read")
      if candidates.empty?
        raise ActiveStorage::FileNotFoundError.new(key)
      end

      last_error = nil

      each_target_to_try(candidates) do |target|
        result = yield(target)
        health.success(target.name)
        return result
      rescue ActiveStorage::FileNotFoundError => e
        health.answered(target.name)
        if ActiveStorageReplicated.config.read_fallback_on_not_found
          last_error ||= e
          next
        end

        raise(last_error || ActiveStorage::FileNotFoundError.new(key))
      rescue StreamInterrupted => e
        raise e.cause
      rescue StandardError => e
        last_error = e
        target_failed(target, :read, key, e)
      end

      raise(last_error || ActiveStorage::FileNotFoundError.new(key))
    end

    def stream_from(target, key)
      streamed = false

      target.download(key) do |chunk|
        streamed = true
        yield chunk
      end
    rescue ActiveStorage::FileNotFoundError
      raise
    rescue StandardError
      if streamed
        raise StreamInterrupted
      end

      raise
    end

    # Signing a link makes no request to the endpoint, so it never spends a trial call.
    def url_target(key)
      fresh = fresh_targets(key, "url")
      first_available_target(fresh.presence || targets)
    end

    # Replaces pending tasks for this key before a write. When the database is unavailable, nothing is busy as
    # far as this process can tell, and pending rows held in memory for the key are replaced.
    def take_over(key, context)
      bookkeeping("#{context} #{key}", default: nil) do
        ActiveStorageReplicated::Task.take_over(service_name: name, key: key, targets: target_names)
      end || begin
        held = ActiveStorageReplicated::Deferred.supersede(service_name: name, key: key, targets: target_names)
        ActiveStorageReplicated::Task::TakeOver.new([], []).tap { |take| take.held = held }
      end
    end

    # An interrupted write may have reached some endpoints, and writers in the pool may still be finishing.
    # Queue a settle for every endpoint a few minutes later, so whatever arrived ends up everywhere.
    SETTLE_DELAY = 5 * 60

    def settle_later(key)
      run_after = Time.current + SETTLE_DELAY
      recorded = bookkeeping("settle #{key}") do
        target_names.map do |target|
          ActiveStorageReplicated::Task.enqueue!(
            service_name: name, target: target, operation: :copy, key: key, run_after: run_after,
            source: ActiveStorageReplicated::Task::SOURCE_RECONCILE,
          )
        end
      end

      if recorded.nil?
        ActiveStorageReplicated::Deferred.add(target_names.map do |target|
          ActiveStorageReplicated::Deferred.row(
            service_name: name, target: target, operation: :copy, key: key, run_after: run_after,
            source: ActiveStorageReplicated::Task::SOURCE_RECONCILE,
          )
        end)
      end
    rescue StandardError
      nil
    end

    # A write that failed everywhere puts back the tasks it replaced, so a pending delete or copy is not lost,
    # including rows that were held in memory. Safe to call more than once.
    def give_back(take, key)
      if take.nil? || take.given_back
        return
      end

      take.given_back = true
      if take.held.present?
        ActiveStorageReplicated::Deferred.add(take.held)
      end

      if take.replaced.empty?
        return
      end

      restored = bookkeeping("restore #{key}") do
        ActiveStorageReplicated::Task.restore(take)
        true
      end

      if restored.nil?
        ActiveStorageReplicated::Deferred.add(take.replaced.map do |row|
          ActiveStorageReplicated::Deferred.row(**row.slice(:service_name, :target, :operation, :key, :source, :created_at, :run_after))
        end)
      end
    end

    # Runs bookkeeping on the to-do table without ever breaking the storage call: a database error is reported
    # and the call carries on, returning `default`. Inside the caller's transaction it uses a savepoint, so the
    # caller's transaction survives the error. Rows that could not be written earlier are written first.
    def bookkeeping(where, default: nil, &block)
      ActiveStorageReplicated.with_writing do
        if ActiveStorageReplicated::Deferred.size.positive?
          ActiveStorageReplicated::Deferred.flush
        end

        if ActiveStorageReplicated.in_app_transaction?
          ActiveStorageReplicated::Task.transaction(requires_new: true, &block)
        else
          yield
        end
      end
    rescue ActiveRecord::ActiveRecordError => e
      ActiveStorageReplicated::Instrumentation.report(e, where: "#{name} #{where}")
      default
    end

    def target_failed(target, operation, key, error)
      health.failure(target.name, error)
      ActiveStorageReplicated::Instrumentation.instrument(
        "operation_failed", service: name.to_s, target: target.name.to_s, operation: operation, key: key, error: error,
      )
      ActiveStorageReplicated.logger.warn(
        "[ActiveStorageReplicated] #{name}/#{target.name} #{operation} #{key} failed: #{error.class}: #{error.message}",
      )
    end
  end
end
