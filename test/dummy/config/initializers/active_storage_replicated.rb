# Every setting with its default. Uncomment a line to change it. See the gem's README for details.
ActiveStorageReplicated.configure do |config|
  # How many endpoints an upload waits for before it returns. The rest are copied in the background.
  # :all waits for every healthy endpoint. 1 is fastest but leaves a single copy for a few seconds.
  # config.sync_copies = :all

  # With sync_copies = :all, write to all endpoints at the same time instead of one after another.
  # config.parallel_writes = true

  # Database for the gem's tables, as named in config/database.yml. nil uses the app's main database.
  # Setting it opens a separate connection pool.
  # config.database = nil

  # Seconds between background runs in each Puma worker.
  # config.interval = 15

  # Endpoint errors in a row before an endpoint is skipped, and seconds before it is probed again.
  # config.failure_threshold = 3
  # config.cool_off = 30

  # Seconds to keep finished tasks. Finished deletes are the proof the audit uses.
  # config.retention = 30 * 24 * 60 * 60

  # Report a task through Rails.error once it has failed this many times.
  # config.alert_after_attempts = 10

  # Daily audit time in the app's time zone, or nil to run it yourself with the rake task.
  # config.audit_at = "03:30"

  # Limit the audit to these key prefixes, for example when several apps share a bucket.
  # config.audit_prefixes = nil
end
