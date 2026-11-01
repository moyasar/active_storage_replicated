require "test_helper"

# Fixes from the fourth review.
class Review4Test < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  test "a flush that cannot get a connection keeps its rows and does not raise" do
    ActiveStorageReplicated::Deferred.add([
      ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "primary", operation: :copy, key: "a")
    ])
    original = ActiveStorageReplicated.method(:in_app_transaction?)
    ActiveStorageReplicated.define_singleton_method(:in_app_transaction?) { raise ActiveRecord::ConnectionTimeoutError, "pool empty" }

    assert_raises(ActiveRecord::ConnectionTimeoutError) { ActiveStorageReplicated::Deferred.flush }
    assert_nothing_raised { @service.upload("b", StringIO.new("x")) }
    assert_equal 1, ActiveStorageReplicated::Deferred.size
  ensure
    ActiveStorageReplicated.define_singleton_method(:in_app_transaction?, original)
  end

  test "a prefix delete without a trailing slash is honoured by reads and the audit" do
    store(@primary, "exports/2024-01.csv", "x")
    File.utime(2.hours.ago.to_time, 2.hours.ago.to_time, @primary.path_for("exports/2024-01.csv"))
    enqueue("primary", :delete_prefixed, "exports/2024")

    assert_raises(ActiveStorage::FileNotFoundError) { @service.download("exports/2024-01.csv") }
    assert_equal 1, ActiveStorageReplicated::Audit.new(@service).run[:skipped]
  end

  test "a delete record of a prefix without a trailing slash counts as proof" do
    ActiveStorageReplicated::Task.enqueue!(service_name: :replicated_test, target: "primary", operation: :delete_prefixed, key: "exports/2024")
      .update_columns(state: ActiveStorageReplicated::Task.states[:done])

    assert ActiveStorageReplicated::Task.last_deleted_at(service_name: :replicated_test, key: "exports/2024-01.csv")
    assert_nil ActiveStorageReplicated::Task.last_deleted_at(service_name: :replicated_test, key: "exports/2025-01.csv")
  end

  test "an audit delete is dated by the delete record it is based on" do
    store(@replica, "doc.pdf", "x")
    File.utime(3.hours.ago.to_time, 3.hours.ago.to_time, @replica.path_for("doc.pdf"))
    tombstone = enqueue("primary", :delete, "doc.pdf")
    tombstone.update_columns(state: ActiveStorageReplicated::Task.states[:done], created_at: 2.hours.ago)

    ActiveStorageReplicated::Audit.new(@service).run

    task = ActiveStorageReplicated::Task.pending.operation_delete.sole
    assert_in_delta 2.hours.ago, task.created_at, 5
  end

  test "a re-upload after the audit listed a file is not deleted by the audit's delete" do
    store(@replica, "doc.pdf", "old")
    File.utime(3.hours.ago.to_time, 3.hours.ago.to_time, @replica.path_for("doc.pdf"))
    tombstone = enqueue("primary", :delete, "doc.pdf")
    tombstone.update_columns(state: ActiveStorageReplicated::Task.states[:done], created_at: 2.hours.ago)
    ActiveStorageReplicated::Audit.new(@service).run

    store(@replica, "doc.pdf", "re-uploaded")
    ActiveStorageReplicated::Task.pending.update_all(run_after: Time.current)
    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_equal "re-uploaded", read_back(@replica, "doc.pdf")
  end

  test "a delete that failed while the database was down stays pending" do
    store(@primary, "doc.pdf", "x")
    store(@replica, "doc.pdf", "x")
    @replica.down = true

    without_database { @service.delete("doc.pdf") }
    ActiveStorageReplicated::Deferred.flush

    assert_equal [ [ "primary", "done" ], [ "replica", "pending" ] ], ActiveStorageReplicated::Task.order(:target).pluck(:target, :state)
  end

  test "an upload interrupted by a timeout puts back the work it replaced" do
    store(@replica, "doc.pdf", "old")
    enqueue("replica", :delete, "doc.pdf")
    @replica.define_singleton_method(:upload) { |*, **| raise Class.new(Exception), "request timeout" }
    ActiveStorageReplicated.config.parallel_writes = false

    assert_raises(Exception) { @service.upload("doc.pdf", StringIO.new("new")) }

    assert ActiveStorageReplicated::Task.pending.operation_delete.exists?(target: "replica", key: "doc.pdf")
  ensure
    ActiveStorageReplicated.config.parallel_writes = true
  end

  test "an audit copy whose source file is gone is skipped, not kept waiting" do
    task = ActiveStorageReplicated::Task.enqueue!(
      service_name: :replicated_test, target: "replica", operation: :copy, key: "gone", source: "audit:primary",
    )

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_predicate task.reload, :skipped?
  end

  test "two endpoints waiting for each other settle on the newest content" do
    store(@primary, "doc.pdf", "newer")
    store(@replica, "doc.pdf", "older")
    File.utime(2.hours.ago.to_time, 2.hours.ago.to_time, @replica.path_for("doc.pdf"))
    to_primary = enqueue("primary", :copy, "doc.pdf")
    to_replica = enqueue("replica", :copy, "doc.pdf")

    2.times { ActiveStorageReplicated::Runner.run_once([ @service ]) }

    assert_predicate to_primary.reload, :done?
    assert_predicate to_replica.reload, :done?
    assert_equal "newer", read_back(@primary, "doc.pdf")
    assert_equal "newer", read_back(@replica, "doc.pdf")
  end

  test "a restore keeps the replaced task's history" do
    old = enqueue("replica", :delete, "doc.pdf")
    old.update_columns(created_at: 3.hours.ago, attempts: 4)
    @primary.down = true
    @replica.down = true

    assert_raises(Errno::ECONNREFUSED) { @service.upload("doc.pdf", StringIO.new("new")) }

    restored = ActiveStorageReplicated::Task.pending.operation_delete.sole
    assert_in_delta 3.hours.ago, restored.created_at, 5
    assert_equal 4, restored.attempts
  end

  test "rows held in memory are dropped in a forked child" do
    ActiveStorageReplicated::Deferred.add([
      ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "primary", operation: :copy, key: "a")
    ])

    ActiveStorageReplicated::Deferred.forget_after_fork

    assert_equal 0, ActiveStorageReplicated::Deferred.size
  end

  test "a request's delete confirms its own lease before deleting" do
    store(@primary, "doc.pdf", "x")
    store(@replica, "doc.pdf", "x")
    original = ActiveStorageReplicated::Task.method(:renew)
    ActiveStorageReplicated::Task.define_singleton_method(:renew) { |*args, **options| options[:seconds] == 60 ? false : original.call(*args, **options) }

    @service.delete("doc.pdf")

    assert stored?(@primary, "doc.pdf")
    assert stored?(@replica, "doc.pdf")
  ensure
    ActiveStorageReplicated::Task.define_singleton_method(:renew, original)
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
