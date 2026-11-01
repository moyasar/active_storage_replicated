require "test_helper"
require "zlib"

# Fixes from the fifth review.
class Review5Test < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  test "a correction never takes an endpoint that is waiting for its own copy as the truth" do
    store(@replica, "inv.pdf", "v2-only-copy")
    enqueue("primary", :copy, "inv.pdf")
    correction = ActiveStorageReplicated::Task.enqueue!(
      service_name: :replicated_test, target: "replica", operation: :copy, key: "inv.pdf",
      source: ActiveStorageReplicated::Task::SOURCE_RECONCILE,
    )

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_equal "v2-only-copy", read_back(@replica, "inv.pdf")
    assert_predicate correction.reload, :done?
  end

  test "a lease lost before anything was written queues no correction" do
    store(@primary, "a", "x")
    task = enqueue("replica", :copy, "a")
    runner = ActiveStorageReplicated::Runner.new([ @service ])
    claimed = ActiveStorageReplicated::Task.claim(owner: runner.instance_variable_get(:@owner)).sole
    ActiveStorageReplicated::Task.where(id: task.id).update_all(claimed_by: "someone-else")

    runner.send(:perform, claimed)

    assert_equal 1, ActiveStorageReplicated::Task.count
    assert_not stored?(@replica, "a")
  end

  test "a prefix delete that runs late spares files written after it was asked for" do
    store(@replica, "exports/old.csv", "old")
    File.utime(2.hours.ago.to_time, 2.hours.ago.to_time, @replica.path_for("exports/old.csv"))
    task = enqueue("replica", :delete_prefixed, "exports/")
    task.update_columns(created_at: 1.hour.ago)
    store(@replica, "exports/new.csv", "new")

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_not stored?(@replica, "exports/old.csv")
    assert stored?(@replica, "exports/new.csv")
    assert_predicate task.reload, :done?
  end

  test "an endpoint marked down mid-run ends its lane without churning the backlog" do
    200.times { |i| store(@primary, "f#{i}", "x") }
    200.times { |i| enqueue("replica", :copy, "f#{i}") }
    @replica.down = true

    updates = 0
    subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
      updates += 1 if payload[:sql].start_with?("UPDATE")
    end
    ActiveStorageReplicated::Runner.run_once([ @service ])
    ActiveSupport::Notifications.unsubscribe(subscriber)

    assert_operator updates, :<, 20
    assert_equal 3, @replica.calls[:upload]
  end

  test "two overlapping flushes during an outage keep every row" do
    3.times do |i|
      ActiveStorageReplicated::Deferred.add([
        ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "primary", operation: :copy, key: "k#{i}")
      ])
    end
    ActiveStorageReplicated::Task.stub_insert_failure = true

    threads = 4.times.map { Thread.new { ActiveStorageReplicated::Deferred.flush } }
    results = threads.map(&:value)

    assert_equal [ 0, 0, 0, 0 ], results
    assert_equal 3, ActiveStorageReplicated::Deferred.size
  ensure
    ActiveStorageReplicated::Task.stub_insert_failure = false
  end

  test "a wrapper that only exposes a path is read as a stream, not reopened" do
    dir = Dir.mktmpdir
    path = File.join(dir, "data.gz")
    Zlib::GzipWriter.open(path) { |gz| gz.write("plain text") }

    Zlib::GzipReader.open(path) { |reader| @service.upload("unzipped", reader) }

    assert_equal "plain text", read_back(@primary, "unzipped")
    assert_equal "plain text", read_back(@replica, "unzipped")
  ensure
    FileUtils.rm_rf(dir)
  end

  test "an interruption after a successful stream keeps the file and records the missing copies in memory" do
    pending_delete = enqueue("replica", :delete, "doc.csv")
    @replica.down = true
    3.times { ActiveStorageReplicated.health.failure("replica") }
    interruption = Class.new(Exception)
    original = @service.method(:track_upload)
    @service.define_singleton_method(:track_upload) { |*, **| raise interruption, "request timeout" }

    assert_raises(interruption) { @service.stream_upload("doc.csv") { |io| io.write("x") } }

    assert stored?(@primary, "doc.csv")
    assert_predicate pending_delete.reload, :skipped?
    assert_equal 1, ActiveStorageReplicated::Deferred.size
  ensure
    @service.define_singleton_method(:track_upload, original) if original
  end

  test "read_fallback_on_not_found asks the next endpoint" do
    store(@replica, "only-on-replica", "x")

    assert_raises(ActiveStorage::FileNotFoundError) { @service.download("only-on-replica") }

    ActiveStorageReplicated.config.read_fallback_on_not_found = true
    assert_equal "x", @service.download("only-on-replica")
    assert_raises(ActiveStorage::FileNotFoundError) { @service.download("nowhere") }
  ensure
    ActiveStorageReplicated.config.read_fallback_on_not_found = false
  end

  test "the audit checks listing order before it acts, so an unsorted endpoint queues nothing wrong" do
    %w[a b c].each do |key|
      store(@primary, key, key)
      store(@replica, key, key)
      [ @primary, @replica ].each { |target| File.utime(2.hours.ago.to_time, 2.hours.ago.to_time, target.path_for(key)) }
    end
    audit = ActiveStorageReplicated::Audit.new(@service)
    original = audit.method(:listing)
    audit.define_singleton_method(:listing) do |target|
      entries = original.call(target).to_a
      target.name == "replica" ? entries.reverse.each : entries.each
    end

    summary = audit.run

    assert_equal 0, summary[:copies]
    assert_equal 0, ActiveStorageReplicated::Task.count
  end

  test "prefix delete lookups use indexes, for pending work and for delete records" do
    [ :pending, :tombstones ].each do |scope_name|
      plans = ActiveStorageReplicated::Task.transaction do
        now = Time.current
        ActiveStorageReplicated::Task.insert_all!(Array.new(5_000) do |i|
          { service_name: "s", target: "replica", operation: i % 3, key: "variants/#{i}/x", state: i % 2, source: nil,
            run_after: now, attempts: 0, created_at: now, updated_at: now }
        end)
        ActiveStorageReplicated::Task.connection.execute("ANALYZE #{ActiveStorageReplicated::Task.quoted_table_name}")
        ActiveStorageReplicated::Task.connection.execute("SET LOCAL enable_seqscan = off")
        scope = ActiveStorageReplicated::Task.public_send(scope_name).where(service_name: "s")
        ActiveStorageReplicated::Task.covering_prefix_deletes(scope, "variants/abc/1").map do |relation|
          ActiveStorageReplicated::Task.connection.select_values("EXPLAIN #{relation.to_sql}").join("\n")
        end
      end

      assert_match(/Index Cond: .*key/m, plans.first, "folder lookup for #{scope_name}")
      assert_match(/Index/, plans.last, "other prefixes for #{scope_name}")
    end
  end
end
