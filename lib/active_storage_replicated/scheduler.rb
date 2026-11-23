module ActiveStorageReplicated
  # Background loop, one per process that runs it (each Puma worker with the plugin). A probe thread checks
  # endpoints that are down. On every tick the main thread writes bookkeeping kept in memory during a database
  # outage, runs the catch-up, and starts the daily audit in its own thread once due.
  class Scheduler
    INITIAL_DELAY = 30

    class << self
      # Starts the background thread in this process. The Puma plugin calls it in each worker; call it yourself
      # from another server's worker boot hook.
      def start
        Thread.new do
          Thread.current.name = "active-storage-replicated"

          until Rails.application&.initialized?
            sleep(1)
          end

          run_loop
        end
      end

      def run_loop(initial_delay: INITIAL_DELAY + rand(ActiveStorageReplicated.config.interval.to_i))
        start_prober
        sleep(initial_delay)

        loop do
          tick
          sleep(ActiveStorageReplicated.config.interval.to_f)
        end
      end

      # Probes run in their own thread so a long copy never delays noticing that an endpoint is back.
      def start_prober
        prober = Thread.new do
          Thread.current.name = "active-storage-replicated-probe"

          loop do
            sleep([ ActiveStorageReplicated.config.cool_off.to_f / 3, 1 ].max)
            probe_tick
          end
        end

        ActiveStorageReplicated.health.prober_running!(prober)
        prober
      end

      def probe_tick
        unless Rails.application&.initialized?
          return
        end

        if ActiveStorageReplicated.health.down_names.empty?
          return
        end

        Rails.application.executor.wrap { probe(ActiveStorageReplicated.services) }
      rescue StandardError => e
        Instrumentation.report(e, where: "probe")
      end

      def tick
        unless Rails.application&.initialized?
          return
        end

        with_long_running_context do
          ActiveStorageReplicated.with_writing do
            services = ActiveStorageReplicated.services
            Deferred.flush
            Runner.run_once(services)
            start_audit_if_due(services)
          end
        end
      rescue StandardError => e
        Instrumentation.report(e, where: "background tick")
      end

      # One cheap request to each endpoint whose cool-off has passed. Any answer from the endpoint, including
      # "not found", means it is reachable again.
      def probe(services)
        services.flat_map(&:targets).uniq { |target| target.name.to_s }.each do |target|
          unless ActiveStorageReplicated.health.due_for_probe?(target.name)
            next
          end

          begin
            target.exist?(ActiveStorageReplicated.config.probe_key)
            ActiveStorageReplicated.health.success(target.name)
          rescue StandardError => e
            ActiveStorageReplicated.health.failure(target.name, e)
          end
        end
      end

      def start_audit_if_due(services, now: Time.current)
        if @audit_thread&.alive?
          return nil
        end

        due = due_audits(services, now: now)
        if due.empty?
          return nil
        end

        @audit_thread = Thread.new do
          Thread.current.name = "active-storage-replicated-audit"
          with_long_running_context { ActiveStorageReplicated.with_writing { run_audits(due) } }
        rescue StandardError => e
          Instrumentation.report(e, where: "audit thread")
        end
      end

      # Runs every audit that is due now, one after another, in the calling thread. Returns the finished runs.
      def run_audit_if_due(now: Time.current, services: nil)
        services ||= ActiveStorageReplicated.services
        runs = []

        loop do
          due = due_audits(services, now: now)
          if due.empty?
            break
          end

          runs.concat(run_audits(due))
        end

        runs
      end

      def audit_thread
        @audit_thread
      end

      private

      # An audit can run for hours. With code reloading on (development), holding the executor that long would
      # block every reload, so only a database connection is held.
      def with_long_running_context(&block)
        if Rails.application.config.respond_to?(:reloading_enabled?) && Rails.application.config.reloading_enabled?
          Task.connection_pool.with_connection(&block)
        else
          Rails.application.executor.wrap(&block)
        end
      end

      def due_audits(services, now:)
        time = ActiveStorageReplicated.config.audit_time
        if time.nil? || now < now.change(hour: time.first, min: time.last)
          return []
        end

        services.select { |service| service.targets.size > 1 }.each do |service|
          run = AuditRun.claim(service_name: service.name, run_on: now.to_date)
          if run
            return [ [ service, run ] ]
          end
        end

        []
      end

      def run_audits(due)
        due.map do |service, run|
          begin
            run.finish!(Audit.new(service, heartbeat: -> { run.heartbeat! }).run)
          rescue StandardError => e
            run.fail!(e)
            Instrumentation.report(e, where: "audit of #{service.name}")
          end

          run.reload
        end
      end
    end
  end
end
