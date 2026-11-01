require "test_helper"

class ResilienceTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
    ActiveStorage::Current.url_options = { host: "example.com" }
  end

  test "an upload succeeds when the to-do table is unavailable" do
    without_database do
      @service.upload("a", StringIO.new("x"))
    end

    assert stored?(@primary, "a")
    assert stored?(@replica, "a")
  end

  test "a delete never raises when the to-do table is unavailable" do
    store(@primary, "a", "x")
    store(@replica, "a", "x")

    without_database do
      assert_nothing_raised { @service.delete("a") }
    end

    assert_not stored?(@primary, "a")
    assert_not stored?(@replica, "a")
  end

  test "reads work when the to-do table is unavailable" do
    store(@primary, "a", "x")

    without_database do
      assert_equal "x", @service.download("a")
    end
  end

  test "an IO that cannot rewind is the caller's problem, not the endpoint's" do
    io = Object.new
    def io.rewind = raise(IOError, "not rewindable")

    assert_raises(IOError) { @service.upload("a", io) }
    assert_not ActiveStorageReplicated.health.down?("primary")
    assert_equal 0, ActiveStorageReplicated.health.instance_variable_get(:@failures).fetch("primary", 0)
  end

  test "errors about one file do not count against the endpoint" do
    error = Class.new(StandardError)
    assert ActiveStorageReplicated::Errors.endpoint_error?(error.new)
    assert_not ActiveStorageReplicated::Errors.endpoint_error?(ActiveStorage::FileNotFoundError.new)
    assert_not ActiveStorageReplicated::Errors.endpoint_error?(ActiveStorage::IntegrityError.new)

    3.times { ActiveStorageReplicated.health.failure("primary", ActiveStorage::IntegrityError.new) }
    assert_not ActiveStorageReplicated.health.down?("primary")
  end

  test "the caller gets its own exception back from stream_upload" do
    custom = Class.new(StandardError)

    assert_raises(custom) { @service.stream_upload("a") { |_io| raise custom } }
  end

  test "parallel writes put the file on every endpoint" do
    file = Tempfile.new("upload")
    file.write("from a file")
    file.flush

    @service.upload("from-file", file)
    @service.upload("from-string", StringIO.new("from a string"))

    assert_equal "from a file", read_back(@replica, "from-file")
    assert_equal "from a string", read_back(@replica, "from-string")
  ensure
    file&.close!
  end

  test "parallel writes still queue a copy for an endpoint that failed" do
    @replica.down = true

    @service.upload("a", StringIO.new("x"))

    assert stored?(@primary, "a")
    assert_equal [ "replica" ], ActiveStorageReplicated::Task.pending.operation_copy.pluck(:target)
  end

  test "an integrity error from any endpoint fails the upload" do
    assert_raises(ActiveStorage::IntegrityError) do
      @service.upload("a", StringIO.new("x"), checksum: Digest::MD5.base64digest("other"))
    end
  end

  test "direct uploads queue delayed copies, and reads skip the endpoints still waiting for them" do
    @service.url_for_direct_upload("direct", expires_in: 5.minutes, content_type: "text/plain",
      content_length: 1, checksum: Digest::MD5.base64digest("x"))

    task = ActiveStorageReplicated::Task.pending.sole
    assert_equal [ "replica", "direct" ], [ task.target, task.key ]
    assert_operator task.run_after, :>, 5.minutes.from_now

    store(@primary, "direct", "x")
    assert_equal "x", @service.download("direct")
    assert_equal 0, @replica.calls[:download]
  end

  test "a direct upload copy waits for a late upload, and is skipped after a day" do
    @service.url_for_direct_upload("late", expires_in: 5.minutes, content_type: "text/plain",
      content_length: 1, checksum: Digest::MD5.base64digest("x"))
    task = ActiveStorageReplicated::Task.sole
    task.update!(run_after: Time.current)

    ActiveStorageReplicated::Runner.run_once([ @service ])
    assert_predicate task.reload, :pending?
    assert_equal 0, task.attempts

    store(@primary, "late", "x")
    task.update!(run_after: Time.current)
    ActiveStorageReplicated::Runner.run_once([ @service ])
    assert_predicate task.reload, :done?
    assert_equal "x", read_back(@replica, "late")
  end

  test "a direct upload that never happened is skipped after a day" do
    @service.url_for_direct_upload("abandoned", expires_in: 5.minutes, content_type: "text/plain",
      content_length: 1, checksum: Digest::MD5.base64digest("x"))
    ActiveStorageReplicated::Task.update_all(run_after: Time.current, created_at: 25.hours.ago)

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_predicate ActiveStorageReplicated::Task.sole, :skipped?
  end

  test "with a prober running, requests never take the trial call" do
    3.times { ActiveStorageReplicated.health.failure("replica") }
    down_until = ActiveStorageReplicated.health.instance_variable_get(:@down_until)
    down_until["replica"] = 0
    prober = Thread.new { sleep }
    ActiveStorageReplicated.health.prober_running!(prober)

    assert_not ActiveStorageReplicated.health.available?("replica")
    @service.upload("a", StringIO.new("x"))
    assert_equal 0, @replica.calls[:upload]

    ActiveStorageReplicated::Scheduler.probe([ @service ])
    assert_not ActiveStorageReplicated.health.down?("replica")
  ensure
    prober&.kill
    ActiveStorageReplicated.health.prober_running!(nil)
  end

  test "a probe of an endpoint that is still down keeps it down" do
    3.times { ActiveStorageReplicated.health.failure("replica") }
    ActiveStorageReplicated.health.instance_variable_get(:@down_until)["replica"] = 0
    @replica.down = true

    ActiveStorageReplicated::Scheduler.probe([ @service ])

    assert ActiveStorageReplicated.health.down?("replica")
    assert_not ActiveStorageReplicated.health.due_for_probe?("replica")
  end

  test "a prober thread that died hands trials back to requests" do
    prober = Thread.new { }
    prober.join
    ActiveStorageReplicated.health.prober_running!(prober)

    assert_not ActiveStorageReplicated.health.prober_running?
  ensure
    ActiveStorageReplicated.health.prober_running!(nil)
  end

  test "health changes are published" do
    events = []
    subscriber = ActiveSupport::Notifications.subscribe(/endpoint_(down|up)\.active_storage_replicated/) { |name, *| events << name }

    3.times { ActiveStorageReplicated.health.failure("replica") }
    ActiveStorageReplicated.health.success("replica")

    assert_equal [ "endpoint_down.active_storage_replicated", "endpoint_up.active_storage_replicated" ], events
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber)
  end

  test "a task that keeps failing is reported once" do
    events = []
    subscriber = ActiveSupport::Notifications.subscribe("task_stuck.active_storage_replicated") { |*args| events << args }
    task = enqueue("replica", :copy, "a")
    task.update_columns(attempts: ActiveStorageReplicated.config.alert_after_attempts - 1)

    task.retry_later!(RuntimeError.new("boom"))
    task.retry_later!(RuntimeError.new("boom"))

    assert_equal 1, events.size
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber)
  end

  test "status shows pending work and the last audit" do
    enqueue("replica", :copy, "a", service_name: :replicated)
    service = ActiveStorage::Blob.services.fetch(:replicated)

    status = ActiveStorageReplicated::Status.new([ service ]).to_h["replicated"]

    assert_equal 1, status[:pending].values.sum
    assert_not_nil status[:oldest_pending_at]
    assert_equal 0, status[:stuck]
  end

  private

  def without_database
    tasks = ActiveStorageReplicated::Task.singleton_class
    names = %i[take_over take_over_prefix enqueue! enqueue_copies pending_operations pending_prefix_delete_targets]
    names.each { |name| tasks.alias_method(:"original_#{name}", name) }
    names.each { |name| tasks.define_method(name) { |*, **| raise ActiveRecord::ConnectionNotEstablished, "database is down" } }
    yield
  ensure
    names.each { |name| tasks.alias_method(name, :"original_#{name}") }
  end
end
