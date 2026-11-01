module ActiveStorageReplicated
  # Settings for the replicated service, the catch-up runner and the daily audit. Every setter checks its value,
  # so a mistake fails at boot instead of in production traffic.
  class Configuration
    attr_accessor :database, :logger
    attr_reader :table_name,
      :audits_table_name,
      :interval,
      :failure_threshold,
      :cool_off,
      :retention,
      :lease,
      :batch_size,
      :time_budget,
      :max_backoff,
      :audit_at,
      :audit_min_age,
      :audit_prefixes,
      :audit_sort_limit,
      :audit_compare_checksums,
      :read_fallback_on_not_found,
      :sync_copies,
      :parallel_writes,
      :alert_after_attempts,
      :direct_upload_grace,
      :probe_key

    def initialize
      @database = nil
      @logger = nil
      @table_name = "active_storage_replicated_tasks"
      @audits_table_name = "active_storage_replicated_audits"
      @interval = 15
      @failure_threshold = 3
      @cool_off = 30
      @retention = 30 * 24 * 60 * 60
      @lease = 10 * 60
      @batch_size = 100
      @time_budget = 50
      @max_backoff = 15 * 60
      @audit_at = "03:30"
      @audit_min_age = 60 * 60
      @audit_prefixes = nil
      @audit_sort_limit = 200_000
      @audit_compare_checksums = true
      @read_fallback_on_not_found = false
      @sync_copies = :all
      @parallel_writes = true
      @alert_after_attempts = 10
      @direct_upload_grace = 60
      @probe_key = "active_storage_replicated/health-check"
    end

    # Checks that need more than one setting, run once the app has booted so the order of assignments does not
    # matter.
    def validate!
      if lease <= time_budget
        raise ArgumentError, "lease (#{lease}) must be longer than time_budget (#{time_budget}), or claimed tasks expire before the runner reaches them"
      end

      self
    end

    def table_name=(value)
      @table_name = require_name(:table_name, value)
    end

    def audits_table_name=(value)
      @audits_table_name = require_name(:audits_table_name, value)
    end

    def interval=(value)
      @interval = require_number(:interval, value, minimum: 1)
    end

    def failure_threshold=(value)
      @failure_threshold = require_integer(:failure_threshold, value, minimum: 1)
    end

    def cool_off=(value)
      @cool_off = require_number(:cool_off, value, minimum: 1)
    end

    def retention=(value)
      @retention = require_number(:retention, value, minimum: 60)
    end

    def lease=(value)
      @lease = require_number(:lease, value, minimum: 30)
    end

    def batch_size=(value)
      @batch_size = require_integer(:batch_size, value, minimum: 1)
    end

    def time_budget=(value)
      @time_budget = require_number(:time_budget, value, minimum: 1)
    end

    def max_backoff=(value)
      @max_backoff = require_number(:max_backoff, value, minimum: 30)
    end

    def audit_min_age=(value)
      @audit_min_age = require_number(:audit_min_age, value, minimum: 0)
    end

    def audit_sort_limit=(value)
      @audit_sort_limit = require_integer(:audit_sort_limit, value, minimum: 1)
    end

    def alert_after_attempts=(value)
      @alert_after_attempts = require_integer(:alert_after_attempts, value, minimum: 1)
    end

    def direct_upload_grace=(value)
      @direct_upload_grace = require_number(:direct_upload_grace, value, minimum: 0)
    end

    def probe_key=(value)
      @probe_key = require_name(:probe_key, value)
    end

    # Turn off for S3-compatible stores whose ETags are not MD5 checksums even for plain uploads.
    def audit_compare_checksums=(value)
      unless value == true || value == false
        raise ArgumentError, "audit_compare_checksums must be true or false, got #{value.inspect}"
      end

      @audit_compare_checksums = value
    end

    # Ask the next endpoint when one answers "not found". Costs one more request per real 404, and covers files
    # an endpoint lost outside the gem's knowledge.
    def read_fallback_on_not_found=(value)
      unless value == true || value == false
        raise ArgumentError, "read_fallback_on_not_found must be true or false, got #{value.inspect}"
      end

      @read_fallback_on_not_found = value
    end

    def parallel_writes=(value)
      unless value == true || value == false
        raise ArgumentError, "parallel_writes must be true or false, got #{value.inspect}"
      end

      @parallel_writes = value
    end

    # Daily audit time as "HH:MM" in the app's time zone, or nil to turn the automatic audit off.
    def audit_at=(value)
      if value.nil?
        @audit_at = nil
        return
      end

      match = value.is_a?(String) && value.match(/\A(\d{1,2}):(\d{2})\z/)
      if !match || match[1].to_i > 23 || match[2].to_i > 59
        raise ArgumentError, "audit_at must look like \"03:30\" or be nil, got #{value.inspect}"
      end

      @audit_at = value
    end

    # Key prefixes the audit compares, or nil for the whole bucket. Use it when several apps share a bucket.
    def audit_prefixes=(value)
      if value.nil?
        @audit_prefixes = nil
        return
      end

      prefixes = Array(value)
      if prefixes.empty? || prefixes.any? { |prefix| !prefix.is_a?(String) || prefix.empty? }
        raise ArgumentError, "audit_prefixes must be nil or a list of non-empty strings, got #{value.inspect}"
      end

      overlapping = prefixes.combination(2).find { |a, b| a.start_with?(b) || b.start_with?(a) }
      if overlapping
        raise ArgumentError, "audit_prefixes must not overlap, got #{overlapping.inspect}"
      end

      @audit_prefixes = prefixes
    end

    # How many endpoints an upload waits for before it returns. The rest are copied by the catch-up.
    def sync_copies=(value)
      if value == :all || (value.is_a?(Integer) && value.positive?)
        @sync_copies = value
      else
        raise ArgumentError, "sync_copies must be :all or a positive integer, got #{value.inspect}"
      end
    end

    def audit_time
      if audit_at.nil?
        return nil
      end

      audit_at.split(":").map(&:to_i)
    end

    private

    def require_number(name, value, minimum:)
      if !value.is_a?(Numeric) || value < minimum
        raise ArgumentError, "#{name} must be a number of at least #{minimum}, got #{value.inspect}"
      end

      value
    end

    def require_integer(name, value, minimum:)
      if !value.is_a?(Integer) || value < minimum
        raise ArgumentError, "#{name} must be an integer of at least #{minimum}, got #{value.inspect}"
      end

      value
    end

    def require_name(name, value)
      if !value.is_a?(String) || value.strip.empty?
        raise ArgumentError, "#{name} must be a non-empty string, got #{value.inspect}"
      end

      value
    end
  end
end
