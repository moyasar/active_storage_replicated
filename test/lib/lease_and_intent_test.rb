require "test_helper"

# Leases, late writes, and keeping the latest intent for a key when operations overlap.
class LeaseAndIntentTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  test "a runner whose lease is gone before it writes does not write" do
    store(@primary, "a", "x")
    task = enqueue("replica", :copy, "a")
    runner = ActiveStorageReplicated::Runner.new([ @service ])
    claimed = ActiveStorageReplicated::Task.claim(owner: runner.instance_variable_get(:@owner)).sole
    original = @primary.method(:download)
    @primary.define_singleton_method(:download) do |*args, **options, &block|
      ActiveStorageReplicated::Task.where(id: task.id).update_all(claimed_by: "someone-else")
      original.call(*args, **options, &block)
    end

    assert_equal :busy, runner.send(:perform, claimed)
    assert_not stored?(@replica, "a")
  end

  test "a result that could not be recorded queues a correction that matches the other endpoints" do
    store(@primary, "doc.pdf", "x")
    task = enqueue("replica", :copy, "doc.pdf")
    runner = ActiveStorageReplicated::Runner.new([ @service ])
    claimed = ActiveStorageReplicated::Task.claim(owner: runner.instance_variable_get(:@owner)).sole
    original = @replica.method(:upload)
    @replica.define_singleton_method(:upload) do |*args, **options|
      original.call(*args, **options)
      ActiveStorageReplicated::Task.where(id: task.id).update_all(state: ActiveStorageReplicated::Task.states[:skipped])
    end

    runner.send(:perform, claimed)

    correction = ActiveStorageReplicated::Task.pending.where(target: "replica", key: "doc.pdf").sole
    assert_predicate correction, :reconcile?
  end

  test "a correction deletes the endpoint's copy when a delete is on record and no other endpoint has the file" do
    store(@replica, "doc.pdf", "late-copy")
    File.utime(2.hours.ago.to_time, 2.hours.ago.to_time, @replica.path_for("doc.pdf"))
    enqueue("primary", :delete, "doc.pdf").update_columns(state: ActiveStorageReplicated::Task.states[:done], created_at: 1.hour.ago)
    ActiveStorageReplicated::Task.enqueue!(
      service_name: :replicated_test, target: "replica", operation: :copy, key: "doc.pdf",
      source: ActiveStorageReplicated::Task::SOURCE_RECONCILE,
    )

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_not stored?(@replica, "doc.pdf")
  end

  test "a correction keeps the only copy when no delete is on record, and copies it to the others" do
    store(@replica, "doc.pdf", "only-copy")
    ActiveStorageReplicated::Task.enqueue!(
      service_name: :replicated_test, target: "replica", operation: :copy, key: "doc.pdf",
      source: ActiveStorageReplicated::Task::SOURCE_RECONCILE,
    )

    2.times { ActiveStorageReplicated::Runner.run_once([ @service ]) }

    assert_equal "only-copy", read_back(@replica, "doc.pdf")
    assert_equal "only-copy", read_back(@primary, "doc.pdf")
  end

  test "a correction copies the current file when another endpoint has newer content" do
    store(@primary, "doc.pdf", "v3")
    store(@replica, "doc.pdf", "v1-late")
    File.utime(2.hours.ago.to_time, 2.hours.ago.to_time, @replica.path_for("doc.pdf"))
    ActiveStorageReplicated::Task.enqueue!(
      service_name: :replicated_test, target: "replica", operation: :copy, key: "doc.pdf",
      source: ActiveStorageReplicated::Task::SOURCE_RECONCILE,
    )

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_equal "v3", read_back(@replica, "doc.pdf")
  end

  test "a correction waits while another endpoint cannot be asked" do
    store(@replica, "doc.pdf", "late-copy")
    task = ActiveStorageReplicated::Task.enqueue!(
      service_name: :replicated_test, target: "replica", operation: :copy, key: "doc.pdf",
      source: ActiveStorageReplicated::Task::SOURCE_RECONCILE,
    )
    @primary.down = true

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_predicate task.reload, :pending?
    assert stored?(@replica, "doc.pdf")
  end

  test "a delete kept for later is skipped if the file was written after the delete was asked for" do
    store(@replica, "invoices/1.pdf", "v2")
    task = ActiveStorageReplicated::Task.enqueue!(
      service_name: :replicated_test, target: "replica", operation: :delete, key: "invoices/1.pdf",
      source: ActiveStorageReplicated::Task::SOURCE_DEFERRED,
    )
    task.update_columns(created_at: 1.hour.ago)

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_predicate task.reload, :skipped?
    assert stored?(@replica, "invoices/1.pdf")
  end

  test "an ordinary delete runs whatever the file's age" do
    store(@replica, "invoices/1.pdf", "v1")
    task = enqueue("replica", :delete, "invoices/1.pdf")
    task.update_columns(created_at: 1.hour.ago)

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_predicate task.reload, :done?
    assert_not stored?(@replica, "invoices/1.pdf")
  end

  test "an upload that fails everywhere puts back the pending delete it replaced" do
    store(@replica, "doc.pdf", "old")
    pending_delete = enqueue("replica", :delete, "doc.pdf")
    @primary.down = true
    @replica.down = true

    assert_raises(Errno::ECONNREFUSED) { @service.upload("doc.pdf", StringIO.new("new")) }

    assert_predicate pending_delete.reload, :skipped?
    assert ActiveStorageReplicated::Task.pending.operation_delete.exists?(target: "replica", key: "doc.pdf")
  end

  test "a newer upload replaces a delete still held in memory" do
    ActiveStorageReplicated::Deferred.add([
      ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "primary", operation: :delete, key: "inv.pdf"),
      ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "replica", operation: :delete, key: "inv.pdf", state: :done)
    ])

    @service.upload("inv.pdf", StringIO.new("v2"))
    ActiveStorageReplicated::Deferred.flush

    assert_not ActiveStorageReplicated::Task.pending.exists?(key: "inv.pdf")
    assert ActiveStorageReplicated::Task.done.exists?(target: "replica", key: "inv.pdf")
    assert stored?(@primary, "inv.pdf")
  end

  test "rows held in memory are never written inside the app's transaction" do
    ActiveStorageReplicated::Deferred.add([
      ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "primary", operation: :copy, key: "a")
    ])

    ActiveStorageReplicated::Task.transaction do
      assert_equal 0, ActiveStorageReplicated::Deferred.flush
      @service.download("missing") rescue ActiveStorage::FileNotFoundError
      raise ActiveRecord::Rollback
    end

    assert_equal 1, ActiveStorageReplicated::Deferred.size
    assert_equal 1, ActiveStorageReplicated::Deferred.flush
  end

  test "bookkeeping uses the writing role even inside a reading block" do
    ActiveRecord::Base.connected_to(role: :reading) do
      @service.upload("a", StringIO.new("x"))
      @replica.down = true
      @service.upload("b", StringIO.new("x"))
    end

    assert ActiveStorageReplicated::Task.pending.exists?(target: "replica", key: "b")
  end

  test "a lease taken by a request lasts a minute, on the database clock" do
    task = ActiveStorageReplicated::Task.enqueue!(service_name: :s, target: "t", operation: :delete, key: "k", owner: "request")

    assert_in_delta 60, task.reload.claimed_until - Time.current, 5
  end

  test "the audit leaves a key alone while a folder delete is pending for it" do
    store(@primary, "variants/abc/1", "x")
    File.utime(2.hours.ago.to_time, 2.hours.ago.to_time, @primary.path_for("variants/abc/1"))
    enqueue("replica", :delete_prefixed, "variants/abc/")

    assert_equal 1, ActiveStorageReplicated::Audit.new(@service).run[:skipped]
  end

  test "a copy whose named source is no longer configured uses any other endpoint" do
    store(@primary, "a", "x")
    ActiveStorageReplicated::Task.enqueue!(service_name: :replicated_test, target: "replica", operation: :copy, key: "a", source: "removed")

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert stored?(@replica, "a")
  end

  test "a full local disk is not blamed on the endpoint" do
    store(@primary, "a", "x")
    task = enqueue("replica", :copy, "a")
    runner = ActiveStorageReplicated::Runner.new([ @service ])
    runner.define_singleton_method(:with_temporary_file) { |_key| raise ActiveStorageReplicated::Errors::LocalFailure, "disk full" }

    claimed = ActiveStorageReplicated::Task.claim(owner: runner.instance_variable_get(:@owner)).sole
    runner.send(:perform, claimed)

    assert_equal 1, task.reload.attempts
    assert_equal 0, ActiveStorageReplicated.health.instance_variable_get(:@failures).fetch("primary", 0)
    assert_equal 0, ActiveStorageReplicated.health.instance_variable_get(:@failures).fetch("replica", 0)
  end

  test "each claim reads its lane from the queue index" do
    plan = ActiveStorageReplicated::Task.transaction do
      connection = ActiveStorageReplicated::Task.connection
      connection.execute(<<~SQL)
        INSERT INTO active_storage_replicated_tasks (service_name, target, operation, key, state, attempts, run_after, created_at, updated_at)
        SELECT 's', 'other', 0, 'k' || g, 0, 0, now(), now(), now() FROM generate_series(1, 20000) g
      SQL
      connection.execute("ANALYZE active_storage_replicated_tasks")
      connection.execute("SET LOCAL enable_seqscan = off")
      sql = ActiveStorageReplicated::Task.claimable_lane(service_name: "s", target: "t", operation: :copy).limit(100).to_sql
      ActiveStorageReplicated::Task.connection.select_values("EXPLAIN #{sql}").join("\n")
    end

    assert_match(/index_active_storage_replicated_tasks_queue/, plan)
  end
end
