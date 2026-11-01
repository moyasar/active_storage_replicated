require "test_helper"

class Review7Test < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  teardown do
    ActiveStorageReplicated::Task.stub_insert_failure = false
    ActiveStorageReplicated::Deferred.clear
  end

  test "an upload that fails everywhere during a database outage keeps the rows held in memory" do
    ActiveStorageReplicated::Deferred.add([
      ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "replica", operation: :delete, key: "doc.pdf")
    ])
    ActiveStorageReplicated::Task.stub_insert_failure = true
    tasks = ActiveStorageReplicated::Task.singleton_class
    tasks.send(:alias_method, :original_take_over_scope, :take_over_scope)
    tasks.send(:define_method, :take_over_scope) { |*| raise ActiveRecord::ConnectionNotEstablished, "database is down" }
    @primary.down = true
    @replica.down = true

    assert_raises(Errno::ECONNREFUSED) { @service.upload("doc.pdf", StringIO.new("x")) }

    assert_equal 1, ActiveStorageReplicated::Deferred.size
  ensure
    tasks.send(:alias_method, :take_over_scope, :original_take_over_scope)
    tasks.send(:private, :take_over_scope)
  end

  test "an interrupt during a flush puts the held rows back" do
    ActiveStorageReplicated::Deferred.add([
      ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "replica", operation: :delete, key: "doc.pdf")
    ])
    timeout = Class.new(Exception) # rubocop:disable Lint/InheritException
    tasks = ActiveStorageReplicated::Task.singleton_class
    tasks.send(:alias_method, :review7_insert_all!, :insert_all!)
    tasks.send(:define_method, :insert_all!) { |*, **| raise timeout, "request timeout" }

    assert_raises(timeout) { ActiveStorageReplicated::Deferred.flush }

    assert_equal 1, ActiveStorageReplicated::Deferred.size
  ensure
    tasks.send(:alias_method, :insert_all!, :review7_insert_all!)
  end

  test "an error raised before a request is sent does not count against the endpoint" do
    3.times { ActiveStorageReplicated.health.failure("primary", ArgumentError.new("expected params[:metadata] to be a String")) }
    3.times { ActiveStorageReplicated.health.failure("primary", NoMethodError.new("undefined method")) }

    assert_not ActiveStorageReplicated.health.down?("primary")
  end

  test "a lease no longer than the time budget is rejected" do
    config = ActiveStorageReplicated::Configuration.new
    config.lease = 40

    assert_raises(ArgumentError) { config.validate! }

    config.lease = 60
    assert_same config, config.validate!
  end
end

# A request that replaces work while another thread writes held rows must see and replace those rows.
class Review7FlushRaceTest < ActiveSupport::TestCase
  include ReplicationHelpers

  self.use_transactional_tests = false

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  teardown do
    ActiveStorageReplicated::Task.delete_all
    ActiveStorageReplicated::Deferred.clear
  end

  test "an upload during a flush replaces the held delete the flush is writing" do
    store(@replica, "doc.pdf", "old")
    ActiveStorageReplicated::Deferred.add([
      ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "replica", operation: :delete, key: "doc.pdf")
    ])
    inserting = Queue.new
    release = Queue.new
    tasks = ActiveStorageReplicated::Task.singleton_class
    tasks.send(:alias_method, :review7_insert_all!, :insert_all!)
    tasks.send(:define_method, :insert_all!) do |*args, **options|
      inserting << true
      release.pop
      review7_insert_all!(*args, **options)
    end

    flusher = Thread.new { ActiveStorageReplicated::Task.connection_pool.with_connection { ActiveStorageReplicated::Deferred.flush } }
    inserting.pop
    releaser = Thread.new do
      sleep(0.3)
      release << true
    end

    @service.upload("doc.pdf", StringIO.new("new"))
    flusher.join
    releaser.join

    assert_equal 0, ActiveStorageReplicated::Task.pending.operation_delete.count
    ActiveStorageReplicated::Runner.run_once([ @service ])
    assert_equal "new", read_back(@replica, "doc.pdf")
  ensure
    tasks.send(:alias_method, :insert_all!, :review7_insert_all!)
  end
end
