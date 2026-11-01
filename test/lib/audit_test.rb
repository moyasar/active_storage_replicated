require "test_helper"

class AuditTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  test "a matching file needs nothing" do
    store_old(@primary, "a", "same")
    store_old(@replica, "a", "same")

    assert_equal({ checked: 1, copies: 0, deletes: 0, skipped: 0 }, audit)
    assert_equal 0, ActiveStorageReplicated::Task.count
  end

  test "a file missing on one side is copied there" do
    store_old(@primary, "invoices/1.pdf", "pdf")

    assert_equal 1, audit[:copies]
    task = ActiveStorageReplicated::Task.sole
    assert_equal [ "replica", "invoices/1.pdf" ], [ task.target, task.key ]
    assert_predicate task, :operation_copy?
  end

  test "a file that differs is copied from the endpoint with the newest content" do
    store_old(@primary, "a", "older", age: 3.hours)
    store_old(@replica, "a", "newer-one", age: 2.hours)

    audit

    task = ActiveStorageReplicated::Task.sole
    assert_equal [ "primary", "audit:replica" ], [ task.target, task.source ]
  end

  test "pending work for an endpoint that is no longer configured does not block the audit" do
    store_old(@primary, "a", "x")
    enqueue("removed", :copy, "a")

    assert_equal 1, audit[:copies]
  end

  test "a file only on a later endpoint is copied to the earlier ones" do
    store_old(@replica, "a", "only-here")

    audit

    task = ActiveStorageReplicated::Task.sole
    assert_equal [ "primary", "audit:replica" ], [ task.target, task.source ]
  end

  test "a listing that is not in byte order is sorted in memory" do
    store_old(@primary, "a", "x")
    store_old(@primary, "b", "x")
    audit_object = ActiveStorageReplicated::Audit.new(@service)
    unsorted = [ entry("b"), entry("a") ]
    audit_object.define_singleton_method(:listing) { |target| target.name == "primary" ? unsorted.each : [].each }

    summary = audit_object.run

    assert_equal 2, summary[:checked]
    assert_equal %w[a b], ActiveStorageReplicated::Task.order(:key).pluck(:key)
  end

  test "an unsorted listing bigger than the sort limit stops the audit" do
    original = ActiveStorageReplicated.config.audit_sort_limit
    ActiveStorageReplicated.config.audit_sort_limit = 1
    audit_object = ActiveStorageReplicated::Audit.new(@service)
    unsorted = [ entry("b"), entry("a") ]
    audit_object.define_singleton_method(:listing) { |target| target.name == "primary" ? unsorted.each : [].each }

    assert_raises(ActiveStorageReplicated::Audit::ListingOutOfOrder) { audit_object.run }
  ensure
    ActiveStorageReplicated.config.audit_sort_limit = original
  end

  test "audit_prefixes limits the comparison" do
    store_old(@primary, "invoices/1.pdf", "x")
    store_old(@primary, "other/1.pdf", "x")
    ActiveStorageReplicated.config.audit_prefixes = [ "invoices/" ]

    assert_equal 1, audit[:checked]
    assert_equal [ "invoices/1.pdf" ], ActiveStorageReplicated::Task.pluck(:key)
  ensure
    ActiveStorageReplicated.config.audit_prefixes = nil
  end

  test "calls the heartbeat while it walks the listings" do
    beats = 0
    stub_const_every = ActiveStorageReplicated::Audit::HEARTBEAT_EVERY
    ActiveStorageReplicated::Audit.send(:remove_const, :HEARTBEAT_EVERY)
    ActiveStorageReplicated::Audit.const_set(:HEARTBEAT_EVERY, 2)
    4.times { |i| store_old(@primary, "f#{i}", "x") }

    ActiveStorageReplicated::Audit.new(@service, heartbeat: -> { beats += 1 }).run

    assert_equal 4, beats # two during the order check, two during the walk
  ensure
    ActiveStorageReplicated::Audit.send(:remove_const, :HEARTBEAT_EVERY)
    ActiveStorageReplicated::Audit.const_set(:HEARTBEAT_EVERY, stub_const_every)
  end

  test "a leftover of a recorded delete is deleted, not copied back" do
    store_old(@replica, "flows/1/a.pdf", "a", age: 3.hours)
    tombstone("primary", :delete, "flows/1/a.pdf", at: 2.hours.ago)

    assert_equal 1, audit[:deletes]
    task = ActiveStorageReplicated::Task.pending.sole
    assert_equal "replica", task.target
    assert_predicate task, :operation_delete?
  end

  test "a prefix delete counts as a delete record" do
    store_old(@replica, "variants/abc/1", "a", age: 3.hours)
    tombstone("primary", :delete_prefixed, "variants/abc/", at: 2.hours.ago)

    assert_equal 1, audit[:deletes]
  end

  test "a file uploaded again after its delete is copied" do
    store_old(@replica, "invoices/1.pdf", "regenerated", age: 2.hours)
    tombstone("primary", :delete, "invoices/1.pdf", at: 3.hours.ago)

    assert_equal 1, audit[:copies]
    assert_equal "primary", ActiveStorageReplicated::Task.pending.sole.target
  end

  test "young files and files with pending work are left alone" do
    store(@primary, "young", "a")
    store_old(@primary, "busy", "a")
    enqueue("replica", :copy, "busy")

    assert_equal 2, audit[:skipped]
    assert_equal 1, ActiveStorageReplicated::Task.count
  end

  test "walks both listings in key order" do
    %w[a c e].each { |key| store_old(@primary, key, key) }
    %w[b c d].each { |key| store_old(@replica, key, key) }

    assert_equal 5, audit[:checked]
    assert_equal %w[a b d e], ActiveStorageReplicated::Task.order(:key).pluck(:key)
  end

  private

  def audit
    ActiveStorageReplicated::Audit.new(@service).run
  end

  def entry(key)
    ActiveStorageReplicated::Audit::Entry.new(key, 1, 2.hours.ago, nil)
  end

  def store_old(target, key, content, age: 2.hours)
    store(target, key, content)
    time = age.ago.to_time
    File.utime(time, time, target.path_for(key))
  end

  def tombstone(target, operation, key, at:)
    enqueue(target, operation, key).update_columns(state: ActiveStorageReplicated::Task.states[:done], created_at: at)
  end

  test "a delete record only seconds newer than the file does not delete it, because clocks can differ" do
    store_old(@replica, "invoices/1.pdf", "regenerated", age: 2.hours)
    tombstone("primary", :delete, "invoices/1.pdf", at: 2.hours.ago + 30.seconds)

    assert_equal 1, audit[:copies]
    assert_equal 0, ActiveStorageReplicated::Task.pending.operation_delete.count
  end
end
