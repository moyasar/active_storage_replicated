module ActiveStorageReplicated
  # Task rows that could not be written because the database was unavailable. They are kept in memory, with
  # the time of the operation they record, and written in this process's order as soon as the database is back,
  # by a retry loop or the background thread, never inside an app transaction. A later write or delete of the
  # same key in this process replaces them first. Pending deletes written this way are marked, and the runner
  # skips them if the file was written after the delete was asked for. Rows still held when the process exits
  # are logged.
  module Deferred
    LIMIT = 10_000
    RETRY_EVERY = 15
    REPORT_EVERY = 60

    @mutex = Mutex.new
    @flushing = Mutex.new
    @rows = []
    @retrier = nil
    @last_report = nil
    @superseded_while_flushing = nil

    at_exit { report_leftovers }

    if defined?(ActiveSupport::ForkTracker)
      ActiveSupport::ForkTracker.after_fork { forget_after_fork }
    end

    module_function

    def add(rows)
      dropped = 0

      @mutex.synchronize do
        @rows.concat(rows)
        if @rows.size > LIMIT
          dropped = @rows.size - LIMIT
          @rows.shift(dropped)
        end
      end

      report(rows.size, dropped)
      start_retrier
    end

    def size
      @mutex.synchronize { @rows.size }
    end

    # Drops pending rows for a key that a newer operation in this process replaces. Finished deletes stay: they
    # are the proof the audit needs.
    # Returns the rows it removed, so a write that then fails can put them back.
    def supersede(service_name:, key:, targets:)
      spec = { service_name: service_name.to_s, key: key, targets: targets.map(&:to_s) }

      @mutex.synchronize do
        removed = @rows.select { |row| superseded_by?(row, spec) }
        @rows -= removed
        @superseded_while_flushing&.push(spec)
        removed
      end
    end

    def superseded_by?(row, spec)
      row[:service_name] == spec[:service_name] && row[:key] == spec[:key] && spec[:targets].include?(row[:target]) &&
        row[:state] == Task.states[:pending]
    end

    def clear
      @mutex.synchronize { @rows.clear }
    end

    # Writes the waiting rows in their original order. Rows stay in memory if the write fails. Does nothing
    # inside an open transaction, which could roll the rows back.
    # One flush at a time in a process; a call that finds another flush running returns at once.
    def flush
      if size.zero? || ActiveStorageReplicated.in_app_transaction?
        return 0
      end

      # The connection is taken before the lock, so requests waiting on the lock never hold the last one.
      Task.connection_pool.with_connection do
        unless @flushing.try_lock
          return 0
        end

        begin
          flush_held_rows
        ensure
          @flushing.unlock
        end
      end
    end

    def flush_held_rows
      rows = @mutex.synchronize do
        @superseded_while_flushing = []
        taken = @rows
        @rows = []
        taken
      end

      begin
        Task.insert_all!(rows.map { |row| row.merge(attempts: 0, updated_at: Time.current) })
        rows.size
      rescue ActiveRecord::ActiveRecordError
        put_back(rows)
        start_retrier
        0
      rescue Exception
        put_back(rows)
        start_retrier
        raise
      ensure
        @mutex.synchronize { @superseded_while_flushing = nil }
      end
    end

    def put_back(rows)
      @mutex.synchronize do
        kept = rows.reject { |row| @superseded_while_flushing.any? { |spec| superseded_by?(row, spec) } }
        @rows = kept + @rows
      end
    end

    # A take-over that runs while another thread is writing held rows waits for that write, so it sees and
    # replaces those rows in the table instead of missing them.
    def wait_for_flush
      if @flushing.owned?
        return
      end

      @flushing.synchronize { nil }
    end

    # In a child process created by forking a process that held rows (Puma's fork_worker), the rows belong to
    # the parent; writing them again here would duplicate them.
    def forget_after_fork
      @mutex = Mutex.new
      @flushing = Mutex.new
      @rows = []
      @retrier = nil
      @superseded_while_flushing = nil
    end

    def row(service_name:, target:, operation:, key:, state: :pending, source: nil, run_after: Time.current, created_at: Time.current)
      deleting = operation.to_s != "copy" && state.to_s == "pending"

      {
        service_name: service_name.to_s,
        target: target.to_s,
        operation: Task.operations.fetch(operation.to_s),
        key: key,
        source: deleting ? Task::SOURCE_DEFERRED : source&.to_s,
        state: Task.states.fetch(state.to_s),
        run_after: run_after,
        created_at: created_at
      }
    end

    def start_retrier
      @mutex.synchronize do
        if @retrier&.alive?
          return
        end

        @retrier = Thread.new do
          Thread.current.name = "active-storage-replicated-deferred"

          loop do
            sleep(RETRY_EVERY)

            begin
              ActiveStorageReplicated.with_writing { Task.connection_pool.with_connection { flush } }
            rescue StandardError => e
              ActiveStorageReplicated.logger.warn("[ActiveStorageReplicated] deferred retry failed: #{e.class}: #{e.message}")
            end

            done = @mutex.synchronize do
              if @rows.empty? && !@flushing.locked?
                @retrier = nil
                true
              end
            end

            if done
              break
            end
          end
        end
      end
    end

    def report_leftovers
      left = size
      if left.positive?
        ActiveStorageReplicated.logger.error(
          "[ActiveStorageReplicated] #{left} task rows were never written because the database stayed unavailable; " \
          "the audit will find the missed copies, and missed deletes may come back",
        )
      end
    rescue StandardError
      nil
    end

    def report(added, dropped)
      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      due = @mutex.synchronize do
        if @last_report && now - @last_report < REPORT_EVERY && dropped.zero?
          false
        else
          @last_report = now
          true
        end
      end

      if due
        Instrumentation.report(
          RuntimeError.new("task rows kept in memory until the database is back: #{added} added, #{size} waiting, #{dropped} dropped"),
          where: "deferred bookkeeping",
        )
      end
    end
  end
end
