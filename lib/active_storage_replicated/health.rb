module ActiveStorageReplicated
  # Per-process, in-memory circuit breaker. Endpoint errors in a row mark an endpoint down for the cool-off.
  # After the cool-off one trial decides whether it is back. When this process runs the background thread,
  # the thread makes that trial with a cheap probe, so user requests never wait on a dead endpoint.
  class Health
    def initialize
      @mutex = Mutex.new
      @prober_thread = nil
      reset!
    end

    # Called by the background thread. Trials belong to it only while it is alive.
    def prober_running!(thread = Thread.current)
      @prober_thread = thread
    end

    def prober_running?
      !@prober_thread.nil? && @prober_thread.alive?
    end

    # True when calls may go to the endpoint. A down endpoint whose cool-off has passed is let through once
    # (a trial), unless a prober owns trials in this process and the caller is not the prober.
    def available?(name, trial: !prober_running?)
      @mutex.synchronize do
        down_until = @down_until[name.to_s]
        if down_until.nil?
          return true
        end

        if !trial || now < down_until
          return false
        end

        @down_until[name.to_s] = now + config.cool_off.to_f
        true
      end
    end

    def down?(name)
      @mutex.synchronize do
        @down_until.key?(name.to_s)
      end
    end

    def due_for_probe?(name)
      @mutex.synchronize do
        down_until = @down_until[name.to_s]
        !down_until.nil? && now >= down_until
      end
    end

    def down_names
      @mutex.synchronize do
        @down_until.keys
      end
    end

    def success(name)
      came_back = @mutex.synchronize do
        @failures.delete(name.to_s)
        !@down_until.delete(name.to_s).nil?
      end

      if came_back
        Instrumentation.instrument("endpoint_up", target: name.to_s)
        ActiveStorageReplicated.logger.info("[ActiveStorageReplicated] #{name} is back up")
      end
    end

    # Records an error. Only endpoint errors count; an error about one file says nothing about the endpoint.
    def failure(name, error = nil)
      if error && !Errors.endpoint_error?(error)
        answered(name)
        return
      end

      went_down = @mutex.synchronize do
        key = name.to_s
        was_down = @down_until.key?(key)
        @failures[key] = @failures.fetch(key, 0) + 1

        if was_down || @failures[key] >= config.failure_threshold
          @down_until[key] = now + config.cool_off.to_f
        end

        !was_down && @down_until.key?(key)
      end

      if went_down
        Instrumentation.instrument("endpoint_down", target: name.to_s, error: error)
        ActiveStorageReplicated.logger.warn("[ActiveStorageReplicated] #{name} is marked down")
      end
    end

    # An answer that is not an endpoint error (a "not found", a rejected checksum) proves the endpoint is
    # reachable, so a down endpoint comes back, but it does not erase endpoint errors already counted.
    def answered(name)
      came_back = @mutex.synchronize { !@down_until.delete(name.to_s).nil? }

      if came_back
        Instrumentation.instrument("endpoint_up", target: name.to_s)
        ActiveStorageReplicated.logger.info("[ActiveStorageReplicated] #{name} is back up")
      end
    end

    def reset!
      @mutex.synchronize do
        @failures = {}
        @down_until = {}
      end
    end

    private

    def config
      ActiveStorageReplicated.config
    end

    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
