module ActiveStorageReplicated
  # Numbers for dashboards and health checks: pending work per endpoint and the last audit of each service.
  class Status
    def initialize(services)
      @services = services
    end

    def to_h
      ActiveStorageReplicated.with_writing { build }
    end

    private

    def build
      @services.to_h do |service|
        pending = Task.pending.where(service_name: service.name.to_s)

        [
          service.name.to_s,
          {
            pending: pending.group(:target, :operation).count.transform_keys { |target, operation| "#{target}/#{operation}" },
            oldest_pending_at: pending.minimum(:created_at),
            stuck: pending.where(attempts: ActiveStorageReplicated.config.alert_after_attempts..).count,
            down_in_this_process: service.targets.map { |target| target.name.to_s }.select { |name| ActiveStorageReplicated.health.down?(name) },
            waiting_in_memory_in_this_process: Deferred.size,
            last_audit: last_audit(service)
          }
        ]
      end
    end

    def last_audit(service)
      run = AuditRun.where(service_name: service.name.to_s).order(run_on: :desc).first
      run&.slice(:run_on, :started_at, :finished_at, :summary, :error, :attempts)
    end
  end
end
