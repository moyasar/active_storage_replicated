require "test_helper"

# Request threads and the runner working on the same file at the same time. A runner "holds" a task by owning
# its active lease. Runs without a wrapping transaction so a second session sees committed rows.
class ConcurrencyTest < ActiveSupport::TestCase
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

  test "a delete while the runner copies the same file is queued behind the copy, and the file stays deleted" do
    store(@primary, "doc.pdf", "secret")
    copy = runner_holds(enqueue("replica", :copy, "doc.pdf"))

    @service.delete("doc.pdf")
    assert_not stored?(@primary, "doc.pdf")

    runner_finishes(copy) { store(@replica, "doc.pdf", "secret") }
    run_runner_until_idle

    assert_not stored?(@primary, "doc.pdf")
    assert_not stored?(@replica, "doc.pdf")
    assert_equal 0, ActiveStorageReplicated::Task.pending.count
  end

  test "a read never serves a file that is waiting to be deleted" do
    store(@primary, "doc.pdf", "secret")
    store(@replica, "doc.pdf", "secret")
    runner_holds(enqueue("replica", :copy, "doc.pdf"))

    @service.delete("doc.pdf")

    assert_raises(ActiveStorage::FileNotFoundError) { @service.download("doc.pdf") }
    assert_not @service.exist?("doc.pdf")
  end

  test "an upload while the runner copies an old version ends with the new version everywhere" do
    store(@primary, "invoices/1.pdf", "v1")
    copy = runner_holds(enqueue("replica", :copy, "invoices/1.pdf"))

    @service.upload("invoices/1.pdf", StringIO.new("v2"))
    assert_equal "v2", read_back(@primary, "invoices/1.pdf")

    runner_finishes(copy) { store(@replica, "invoices/1.pdf", "v1") }
    run_runner_until_idle

    assert_equal "v2", read_back(@primary, "invoices/1.pdf")
    assert_equal "v2", read_back(@replica, "invoices/1.pdf")
  end

  test "an upload never writes an endpoint the runner holds, even when every other endpoint fails" do
    store(@replica, "invoices/1.pdf", "v1")
    runner_holds(enqueue("primary", :copy, "invoices/1.pdf"))
    @replica.down = true

    assert_raises(Errno::ECONNREFUSED) { @service.upload("invoices/1.pdf", StringIO.new("v2")) }
    assert_not stored?(@primary, "invoices/1.pdf")
  end

  test "an upload with every endpoint held fails without writing" do
    runner_holds(enqueue("primary", :copy, "a"))
    runner_holds(enqueue("replica", :copy, "a"))

    assert_raises(ActiveStorage::Service::ReplicatedService::EndpointsBusy) { @service.upload("a", StringIO.new("x")) }
    assert_raises(ActiveStorage::Service::ReplicatedService::EndpointsBusy) { @service.stream_upload("a") { |io| io.write("x") } }
  end

  test "a claimed old delete does not run after a newer upload replaced it" do
    @replica.down = true
    store(@primary, "invoices/1.pdf", "v1")
    @service.delete("invoices/1.pdf")
    @replica.down = false
    ActiveStorageReplicated.health.reset!
    ActiveStorageReplicated::Task.pending.update_all(run_after: Time.current)

    runner = ActiveStorageReplicated::Runner.new([ @service ])
    owner = runner.instance_variable_get(:@owner)
    claimed = ActiveStorageReplicated::Task.claim(owner: owner)
    ActiveStorageReplicated::Task.where(id: claimed.map(&:id)).update_all(claimed_until: 1.second.ago)

    @service.upload("invoices/1.pdf", StringIO.new("v2"))
    claimed.each { |task| runner.send(:perform, task) }

    assert_equal "v2", read_back(@primary, "invoices/1.pdf")
    assert_equal "v2", read_back(@replica, "invoices/1.pdf")
  end

  test "a runner that lost its lease mid-copy does not record a result" do
    store(@primary, "a", "x")
    task = enqueue("replica", :copy, "a")
    runner = ActiveStorageReplicated::Runner.new([ @service ])
    owner = runner.instance_variable_get(:@owner)
    claimed = ActiveStorageReplicated::Task.claim(owner: owner).sole
    original = @replica.method(:upload)
    @replica.define_singleton_method(:upload) do |*args, **options|
      ActiveStorageReplicated::Task.where(id: task.id).update_all(claimed_by: "someone-else")
      original.call(*args, **options)
    end

    assert_equal :busy, runner.send(:perform, claimed)
    assert_predicate task.reload, :pending?
    assert_equal "someone-else", task.claimed_by
  end

  test "a delete of a folder queues exact deletes behind copies in flight under it" do
    store(@primary, "variants/abc/1", "x")
    copy = runner_holds(enqueue("replica", :copy, "variants/abc/1"))

    @service.delete_prefixed("variants/abc/")

    exact = ActiveStorageReplicated::Task.pending.operation_delete.find_by!(target: "replica", key: "variants/abc/1")
    assert_operator exact.run_after, :<=, Time.current

    runner_finishes(copy) { store(@replica, "variants/abc/1", "x") }
    run_runner_until_idle

    assert_not stored?(@replica, "variants/abc/1")
  end

  test "claim skips rows another session is claiming" do
    locked = enqueue("replica", :copy, "a")
    free = enqueue("replica", :copy, "b")
    holding = Queue.new
    release = Queue.new

    holder = Thread.new do
      ActiveStorageReplicated::Task.transaction do
        ActiveStorageReplicated::Task.where(id: locked.id).lock("FOR UPDATE").to_a
        holding << true
        release.pop
      end
    end

    holding.pop
    assert_equal [ free.id ], ActiveStorageReplicated::Task.claim(owner: "w").map(&:id)
  ensure
    release << true
    holder&.join
  end

  test "bookkeeping failures inside the caller's transaction leave that transaction usable" do
    broken = ->(**) { raise ActiveRecord::StatementInvalid, "database hiccup" }
    tasks = ActiveStorageReplicated::Task.singleton_class

    ActiveStorageReplicated::Task.transaction do
      tasks.alias_method(:original_take_over, :take_over)
      tasks.define_method(:take_over, &broken)

      @service.upload("a", StringIO.new("x"))

      assert_equal 0, ActiveStorageReplicated::Task.count
    ensure
      tasks.alias_method(:take_over, :original_take_over)
    end

    assert stored?(@primary, "a")
    assert stored?(@replica, "a")
  end

  private

  # Leases the task to a pretend runner, as a runner does while it works on it.
  def runner_holds(task)
    ActiveStorageReplicated::Task.where(id: task.id).update_all(claimed_by: "runner", claimed_until: 10.minutes.from_now)
    task.reload
  end

  def runner_finishes(task)
    yield
    task.reload.finish!(owner: "runner")
  end

  def run_runner_until_idle
    5.times do
      ActiveStorageReplicated::Task.pending.update_all(run_after: Time.current, claimed_until: nil, claimed_by: nil)
      ActiveStorageReplicated::Runner.run_once([ @service ])
    end
  end
end
