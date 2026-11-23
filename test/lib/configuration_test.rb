require "test_helper"

class ConfigurationTest < ActiveSupport::TestCase
  setup do
    @config = ActiveStorageReplicated::Configuration.new
  end

  test "rejects bad values at assignment" do
    {
      interval: [ 0, -1, "15", nil ],
      failure_threshold: [ 0, 1.5, nil ],
      cool_off: [ 0, nil ],
      lease: [ 10, nil ],
      batch_size: [ 0, 2.5 ],
      time_budget: [ 0 ],
      max_backoff: [ 1 ],
      retention: [ 1 ],
      audit_min_age: [ -1 ],
      alert_after_attempts: [ 0 ],
      audit_sort_limit: [ 0, 1.5 ],
      direct_upload_grace: [ -1 ],
      table_name: [ "", nil, :tasks ],
      audits_table_name: [ " " ],
      probe_key: [ "" ],
      parallel_writes: [ nil, "yes" ],
      audit_compare_checksums: [ nil, 1 ],
      audit_at: [ "3", "25:00", "03:60", :daily ],
      audit_prefixes: [ [], [ "" ], [ "a/", "a/b/" ], [ 1 ] ],
      sync_copies: [ 0, "1", :some, nil ],
      redirect_url_options: [ "api.example.com", [ :host ] ]
    }.each do |setting, values|
      values.each do |value|
        assert_raises(ArgumentError, "#{setting} = #{value.inspect}") { @config.public_send(:"#{setting}=", value) }
      end
    end
  end

  test "accepts good values" do
    @config.interval = 0.5 + 0.5
    @config.audit_at = "23:59"
    @config.audit_at = nil
    @config.audit_prefixes = [ "invoices/", "statements/" ]
    @config.audit_prefixes = nil
    @config.sync_copies = 2
    @config.parallel_writes = false

    assert_equal 2, @config.sync_copies
    assert_nil @config.audit_time
  end
end
