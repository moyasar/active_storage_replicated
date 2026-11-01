require "test_helper"

# The database and an endpoint failing at the same time. Bookkeeping that could not be written is kept in
# memory and written once the database is back.
class DatabaseOutageTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
    ActiveStorageReplicated::Deferred.clear
  end

  teardown do
    ActiveStorageReplicated::Deferred.clear
  end

  test "an upload during a database outage that missed an endpoint gets its copy once the database is back" do
    @primary.down = true

    without_database { @service.upload("a", StringIO.new("x")) }

    assert stored?(@replica, "a")
    assert_equal 1, ActiveStorageReplicated::Deferred.size

    assert_equal 1, ActiveStorageReplicated::Deferred.flush
    assert_equal [ "primary" ], ActiveStorageReplicated::Task.pending.operation_copy.pluck(:target)

    @primary.down = false
    ActiveStorageReplicated.health.reset!
    assert_equal "x", @service.download("a")
    ActiveStorageReplicated::Runner.run_once([ @service ])
    assert stored?(@primary, "a")
  end

  test "a delete during a database outage keeps its proof and its pending delete" do
    store(@primary, "doc.pdf", "x")
    store(@replica, "doc.pdf", "x")
    @replica.down = true
    3.times { ActiveStorageReplicated.health.failure("replica") }

    without_database { @service.delete("doc.pdf") }
    ActiveStorageReplicated::Deferred.flush

    assert_equal [ [ "primary", "done" ], [ "replica", "pending" ] ],
      ActiveStorageReplicated::Task.order(:target).pluck(:target, :state)

    @replica.down = false
    ActiveStorageReplicated.health.reset!
    ActiveStorageReplicated::Runner.run_once([ @service ])
    assert_not stored?(@replica, "doc.pdf")
  end

  test "rows stay in memory while the database is still down" do
    @primary.down = true
    without_database { @service.upload("a", StringIO.new("x")) }

    ActiveStorageReplicated::Task.stub_insert_failure = true
    assert_equal 0, ActiveStorageReplicated::Deferred.flush
    assert_equal 1, ActiveStorageReplicated::Deferred.size
  ensure
    ActiveStorageReplicated::Task.stub_insert_failure = false
  end

  test "a stream that cannot rewind reaches every endpoint whole" do
    reader, writer = IO.pipe
    writer.write("payload")
    writer.close

    @service.upload("piped", reader)

    assert_equal "payload", read_back(@primary, "piped")
    assert_equal "payload", read_back(@replica, "piped")
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
