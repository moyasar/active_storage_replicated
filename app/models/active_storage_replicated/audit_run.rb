module ActiveStorageReplicated
  # One audit of one service on one day. Exactly one process runs it. A run whose process died is taken over
  # once its heartbeat is stale, and a failed run is retried after an hour. Takeovers and retries together are
  # limited per day, so an audit that keeps killing its worker stops after a few tries.
  class AuditRun < Record
    self.table_name = ActiveStorageReplicated.config.audits_table_name

    STALE_AFTER = 10 * 60
    RETRY_FAILED_AFTER = 60 * 60
    MAX_ATTEMPTS = 8

    class << self
      def claim(service_name:, run_on:)
        now = Time.current
        inserted = insert_all(
          [ { service_name: service_name.to_s, run_on: run_on, started_at: now, heartbeat_at: now, attempts: 1 } ],
          unique_by: [ :service_name, :run_on ],
          returning: [ :id ],
        )

        if inserted.rows.any?
          return find(inserted.rows.first.first)
        end

        take_over(service_name: service_name, run_on: run_on, now: now)
      end

      private

      def take_over(service_name:, run_on:, now:)
        taken = where(service_name: service_name.to_s, run_on: run_on)
          .where(attempts: ...MAX_ATTEMPTS)
          .where(
            "(finished_at IS NULL AND heartbeat_at < now() - make_interval(secs => ?)) OR " \
            "(error IS NOT NULL AND finished_at < now() - make_interval(secs => ?))",
            STALE_AFTER, RETRY_FAILED_AFTER,
          )
          .update_all([ "started_at = ?, heartbeat_at = now(), finished_at = NULL, error = NULL, summary = NULL, " \
                        "attempts = attempts + 1, updated_at = now()", now ])

        if taken.zero?
          return nil
        end

        find_by!(service_name: service_name.to_s, run_on: run_on)
      end
    end

    class TakenOver < StandardError; end

    # The run's `started_at` is its owner token: a takeover changes it, and the old process then stops.
    def heartbeat!
      if mine.update_all("heartbeat_at = now()").zero?
        raise TakenOver, "audit of #{service_name} for #{run_on} was taken over by another process"
      end
    end

    def finish!(summary)
      mine.update_all([ "finished_at = now(), summary = ?, updated_at = now()", summary.to_json ]).positive?
    end

    def fail!(error)
      mine.update_all([ "finished_at = now(), error = ?, updated_at = now()", "#{error.class}: #{error.message}".truncate(2_000) ]).positive?
    end

    private

    def mine
      self.class.where(id: id, started_at: started_at, finished_at: nil)
    end
  end
end
