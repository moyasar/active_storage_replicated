module ActiveStorageReplicated
  # Compares every endpoint of a service and queues tasks for what differs.
  #
  # - A file missing on an endpoint is copied there.
  # - A file that differs is copied from the endpoint with the newest content. Catch-up copies carry the time
  #   their content was first written, so a late copy of an old version does not look new.
  # - A file is deleted only when a recorded delete is clearly newer than every copy of it.
  # - Files younger than `audit_min_age`, and files with pending work, are left alone.
  class Audit
    class ListingOutOfOrder < StandardError; end

    Entry = Struct.new(:key, :size, :modified_at, :etag)

    HEARTBEAT_EVERY = 1_000
    CLOCK_MARGIN = 60
    WRITTEN_AT_METADATA = "replicated-written-at"
    MD5_ETAG = /\A[0-9a-f]{32}\z/

    def self.run(services = ActiveStorageReplicated.services)
      ActiveStorageReplicated.with_writing do
        services.to_h { |service| [ service.name.to_s, new(service).run ] }
      end
    end

    HEARTBEAT_SECONDS = 60

    def initialize(service, heartbeat: nil, now: nil)
      @service = service
      @heartbeat = heartbeat
      @now = now
      @last_beat = monotonic
      @sort_in_memory = false
    end

    # Lists every endpoint once to check that it lists in byte order, before acting on anything. If one does
    # not, as some self-hosted S3 servers do not, every listing is sorted in memory, up to `audit_sort_limit`
    # keys. Acting during a walk that turns out to be unsorted would treat keys as missing that are not.
    def run
      unsorted = @service.targets.find { |target| !sorted_listing?(target) }
      if unsorted
        ActiveStorageReplicated.logger.warn("[ActiveStorageReplicated::Audit] #{unsorted.name} lists out of order; sorting listings in memory")
        @sort_in_memory = true
      end

      walk
    end

    def sorted_listing?(target)
      previous = nil
      count = 0

      listing(target).each do |entry|
        key = entry.key.b
        if previous && key <= previous
          return false
        end

        previous = key
        count += 1
        beat(count)
      end

      true
    end

    private

    def walk
      summary = { checked: 0, copies: 0, deletes: 0, skipped: 0 }

      each_key do |key, entries|
        summary[:checked] += 1
        outcome = reconcile(key, entries)
        if outcome
          summary[outcome] += 1
        end

        beat(summary[:checked])
      end

      ActiveStorageReplicated.logger.info("[ActiveStorageReplicated::Audit] #{@service.name}: #{summary}")
      Instrumentation.instrument("audit", service: @service.name.to_s, summary: summary)
      summary
    end

    def reconcile(key, entries)
      first_holder = @service.targets.find { |target| entries.key?(target) }
      behind = @service.targets.reject { |target| same?(target, entries[target], first_holder, entries[first_holder], key) }

      if behind.empty?
        return nil
      end

      newest = entries.values.max_by(&:modified_at)
      if newest.modified_at > (@now || Time.current) - ActiveStorageReplicated.config.audit_min_age.to_i
        return :skipped
      end

      if Task.pending_for?(service_name: @service.name, key: key, targets: @service.target_names)
        return :skipped
      end

      written_at = entries.to_h { |target, entry| [ target, written_at(target, key, entry) ] }
      if written_at.values.any?(&:nil?)
        return :skipped
      end

      reference_target = newest_content(written_at)
      reference = entries[reference_target]
      behind = @service.targets.reject { |target| same?(target, entries[target], reference_target, reference, key) }

      # The delete record uses the app's clock and the file time uses the endpoint's clock, so a delete must be
      # clearly newer than the file. When in doubt the file is kept.
      # Judged by when each copy's content was first written, so a copy that landed late does not look newer
      # than a delete asked for after its content was written.
      deleted_at = Task.last_deleted_at(service_name: @service.name, key: key)
      if deleted_at && deleted_at > written_at.values.max + CLOCK_MARGIN
        entries.each_key do |target|
          Task.enqueue!(
            service_name: @service.name, target: target.name, operation: :delete, key: key,
            source: Task::SOURCE_AUDIT, created_at: deleted_at,
          )
        end

        return :deletes
      end

      behind.each do |target|
        Task.enqueue!(
          service_name: @service.name, target: target.name, operation: :copy, key: key,
          source: "#{Task::AUDIT_COPY_PREFIX}#{reference_target.name}",
        )
      end

      :copies
    end

    # The endpoint whose copy was written last, judged by the original write time a catch-up copy carries,
    # or the file time otherwise. Ties go to the first endpoint in `targets` order.
    def newest_content(written_at)
      latest = written_at.values.max

      @service.targets.find { |target| written_at[target] == latest }
    end

    # When this endpoint's copy was first written, read fresh rather than from a listing that may be hours old.
    # Nil when it cannot be read or the file is gone; the key is then left for the next audit.
    def written_at(target, key, entry)
      unless s3_target?(target)
        path = target.respond_to?(:path_for) ? target.path_for(key) : nil
        return path && File.exist?(path) ? File.mtime(path) : nil
      end

      object = target.bucket.object(key)
      object.load
      stamp = object.metadata[WRITTEN_AT_METADATA]
      stamp ? Time.iso8601(stamp) : object.last_modified
    rescue StandardError
      nil
    end

    # Every HEARTBEAT_EVERY keys, or every minute when keys are slow.
    def beat(count)
      if @heartbeat.nil?
        return
      end

      if (count % HEARTBEAT_EVERY).zero? || monotonic - @last_beat >= HEARTBEAT_SECONDS
        @last_beat = monotonic
        @heartbeat.call
      end
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def s3_target?(target)
      defined?(ActiveStorage::Service::S3Service) && target.is_a?(ActiveStorage::Service::S3Service)
    end

    def same?(target, entry, reference_target, reference, key)
      if entry.nil?
        return false
      end

      if target == reference_target
        return true
      end

      if entry.size != reference.size
        return false
      end

      !checksums_differ?(target, entry, reference_target, reference, key)
    end

    # Listing ETags are compared only when both look like MD5s, and a difference is confirmed with a HEAD on
    # each side, because encrypted or multipart objects have ETags that are not checksums.
    def checksums_differ?(target, entry, reference_target, reference, key)
      unless ActiveStorageReplicated.config.audit_compare_checksums
        return false
      end

      if [ target, reference_target ].any? { |endpoint| Runner.etags_not_md5.include?(endpoint.name.to_s) }
        return false
      end

      unless entry.etag&.match?(MD5_ETAG) && reference.etag&.match?(MD5_ETAG)
        return false
      end

      if entry.etag == reference.etag
        return false
      end

      comparable?(target, key) && comparable?(reference_target, key)
    rescue StandardError
      false
    end

    def comparable?(target, key)
      object = target.bucket.object(key)
      object.load
      object.sse_customer_algorithm.blank? &&
        (object.server_side_encryption.blank? || object.server_side_encryption == "AES256")
    end

    # Walks the sorted listings of all endpoints side by side, yielding each key once with what every
    # endpoint has. Stops with an error if an endpoint does not list keys in byte order, because the walk
    # would then report every file as missing.
    def each_key
      cursors = @service.targets.index_with { |target| ordered(listing(target), target) }

      loop do
        heads = cursors.filter_map do |target, cursor|
          [ target, cursor.peek ]
        rescue StopIteration
          nil
        end

        if heads.empty?
          break
        end

        key = heads.map { |_target, entry| entry.key }.min
        entries = {}

        heads.each do |target, entry|
          if entry.key == key
            entries[target] = entry
            cursors[target].next
          end
        end

        yield key, entries
      end
    end

    def ordered(entries, target)
      if @sort_in_memory
        return sorted_in_memory(entries, target)
      end

      Enumerator.new do |yielder|
        previous = nil

        entries.each do |entry|
          if previous && entry.key.b <= previous
            raise ListingOutOfOrder, "#{target.name} listed #{entry.key.inspect} after #{previous.inspect}"
          end

          previous = entry.key.b
          yielder << entry
        end
      end
    end

    def sorted_in_memory(entries, target)
      limit = ActiveStorageReplicated.config.audit_sort_limit
      all = []

      entries.each do |entry|
        all << entry
        beat(all.size)

        if all.size > limit
          raise ListingOutOfOrder, "#{target.name} lists out of order and has more than #{limit} keys to sort"
        end
      end

      all.sort_by { |entry| entry.key.b }.each
    end

    def listing(target)
      if defined?(ActiveStorage::Service::S3Service) && target.is_a?(ActiveStorage::Service::S3Service)
        s3_listing(target)
      elsif defined?(ActiveStorage::Service::DiskService) && target.is_a?(ActiveStorage::Service::DiskService)
        disk_listing(target)
      else
        raise NotImplementedError, "Listing #{target.class} is not supported"
      end
    end

    def prefixes
      Array(ActiveStorageReplicated.config.audit_prefixes).sort.presence || [ nil ]
    end

    def s3_listing(target)
      Enumerator.new do |yielder|
        prefixes.each do |prefix|
          objects = prefix ? target.bucket.objects(prefix: prefix) : target.bucket.objects

          objects.each do |object|
            yielder << Entry.new(object.key, object.size, object.last_modified, object.etag.to_s.delete('"'))
          end
        end
      end
    end

    # Disk keys live under two folders built from their first four characters.
    def disk_listing(target)
      root = Pathname.new(target.root)
      entries = Dir.glob("**/*", base: root.to_s).filter_map do |relative|
        path = root.join(relative)
        unless path.file?
          next
        end

        key = relative.split("/", 3).last
        if prefixes.compact.any? && prefixes.compact.none? { |prefix| key.start_with?(prefix) }
          next
        end

        Entry.new(key, path.size, path.mtime, nil)
      end

      entries.sort_by { |entry| entry.key.b }.each
    end
  end
end
