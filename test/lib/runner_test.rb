require "test_helper"

class RunnerTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  test "does nothing without a replicated service" do
    assert_nil ActiveStorageReplicated::Runner.run_once([])
  end

  test "copies a missed upload once the target is back" do
    @replica.down = true
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))
    @replica.down = false

    summary = run_runner

    assert_equal 1, summary[:done]
    assert_equal "pdf", read_back(@replica, "invoices/1.pdf")
    task = ActiveStorageReplicated::Task.sole
    assert_predicate task, :done?
    assert_nil task.claimed_until
  end

  test "runs a missed delete once the target is back" do
    store(@primary, "flows/1/a.pdf", "a")
    store(@replica, "flows/1/a.pdf", "a")
    @replica.down = true
    @service.delete("flows/1/a.pdf")
    @replica.down = false
    ActiveStorageReplicated::Task.pending.update_all(run_after: Time.current)

    run_runner

    assert_not stored?(@replica, "flows/1/a.pdf")
    assert_equal 0, ActiveStorageReplicated::Task.pending.count
  end

  test "tasks for an endpoint marked down are not claimed at all" do
    store(@primary, "a", "a")
    task = enqueue("replica", :copy, "a")
    @replica.down = true
    3.times do
      run_runner
      ActiveStorageReplicated::Task.pending.update_all(run_after: Time.current)
    end
    assert ActiveStorageReplicated.health.down?("replica")

    updates = count_updates { run_runner }

    task.reload
    assert_predicate task, :pending?
    assert_equal 3, task.attempts
    assert_nil task.claimed_until
    assert_equal 0, updates
  end

  test "a large backlog for a down endpoint costs no writes" do
    200.times { |i| enqueue("replica", :copy, "f#{i}") }
    3.times { ActiveStorageReplicated.health.failure("replica") }

    assert_equal 0, count_updates { run_runner }
  end

  test "a failure backs the task off and records the error" do
    store(@primary, "a", "a")
    task = enqueue("replica", :copy, "a")
    @replica.down = true

    run_runner

    task.reload
    assert_equal 1, task.attempts
    assert_match(/endpoint down/, task.last_error)
    assert_in_delta 30.seconds.from_now, task.run_after, 5
  end

  test "a source that is down does not count against the destination" do
    task = enqueue("replica", :copy, "a")
    @primary.down = true

    run_runner

    assert_equal 1, task.reload.attempts
    assert_not ActiveStorageReplicated.health.down?("replica")
    assert_equal 0, ActiveStorageReplicated.health.instance_variable_get(:@failures).fetch("replica", 0)
  end

  test "skips a copy whose file is gone from every other target" do
    task = enqueue("replica", :copy, "gone")

    run_runner

    assert_predicate task.reload, :skipped?
  end

  test "tasks for one file run in the order they were recorded" do
    store(@primary, "a", "v1")
    first = enqueue("replica", :copy, "a")
    second = enqueue("replica", :delete, "a")
    first.update!(run_after: 1.hour.from_now)

    run_runner

    assert_predicate second.reload, :pending?

    first.update!(run_after: Time.current)
    run_runner
    assert_predicate first.reload, :done?

    run_runner
    assert_predicate second.reload, :done?
    assert_not stored?(@replica, "a")
  end

  test "leaves tasks for services or targets this process does not know, for another process or deploy" do
    unknown_service = enqueue("replica", :copy, "a", service_name: :nope)
    unknown_target = enqueue("nope", :copy, "a")

    real = enqueue("replica", :copy, "real")
    store(@primary, "real", "x")

    run_runner

    [ unknown_service, unknown_target ].each do |task|
      task.reload
      assert_predicate task, :pending?
      assert_nil task.claimed_by
    end
    assert_predicate real.reload, :done?
  end

  test "many tasks for unknown endpoints never hold back real work" do
    150.times { |i| enqueue("nope", :copy, "f#{i}") }
    store(@primary, "real", "x")
    real = enqueue("replica", :copy, "real")

    run_runner

    assert_predicate real.reload, :done?
  end

  test "purges finished tasks older than the retention period" do
    old = enqueue("replica", :delete, "a")
    old.update_columns(state: ActiveStorageReplicated::Task.states[:done], updated_at: 31.days.ago)
    recent = enqueue("replica", :delete, "b")
    recent.update_columns(state: ActiveStorageReplicated::Task.states[:done], updated_at: 1.day.ago)

    assert_equal 1, run_runner[:purged]
    assert_not ActiveStorageReplicated::Task.exists?(old.id)
    assert ActiveStorageReplicated::Task.exists?(recent.id)
  end

  private

  def count_updates
    updates = 0
    subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
      if payload[:sql].start_with?("UPDATE")
        updates += 1
      end
    end
    yield
    updates
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber)
  end

  def run_runner
    ActiveStorageReplicated::Runner.run_once([ @service ])
  end
end
