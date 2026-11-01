require "test_helper"

class TaskTest < ActiveSupport::TestCase
  include ReplicationHelpers

  test "claim stamps a lease and hides the tasks from the next claim" do
    first = enqueue("replica", :copy, "a")
    second = enqueue("replica", :copy, "b")

    claimed = ActiveStorageReplicated::Task.claim(owner: "w")

    assert_equal [ first.id, second.id ], claimed.map(&:id)
    assert claimed.all? { |task| task.claimed_until > Time.current }
    assert_empty ActiveStorageReplicated::Task.claim(owner: "w")
  end

  test "a claim whose lease ran out can be taken again" do
    task = enqueue("replica", :copy, "a")
    task.update_columns(claimed_until: 1.minute.ago)

    assert_equal [ task.id ], ActiveStorageReplicated::Task.claim(owner: "w").map(&:id)
  end

  test "a claimed older task still blocks newer tasks for the same file" do
    older = enqueue("replica", :copy, "a")
    older.update_columns(claimed_until: 5.minutes.from_now)
    enqueue("replica", :delete, "a")
    other = enqueue("replica", :copy, "b")

    assert_equal [ other.id ], ActiveStorageReplicated::Task.claim(owner: "w").map(&:id)
  end

  test "claim respects the batch size and the cursor" do
    tasks = 3.times.map { |i| enqueue("replica", :copy, "f#{i}") }

    assert_equal [ tasks[0].id ], ActiveStorageReplicated::Task.claim(owner: "w", limit: 1).map(&:id)
    assert_equal [ tasks[2].id ], ActiveStorageReplicated::Task.claim(owner: "w", after_id: tasks[1].id).map(&:id)
  end

  test "retry_later backs off and caps the delay" do
    task = enqueue("replica", :copy, "a")

    task.retry_later!(RuntimeError.new("boom"))
    assert_in_delta 30.seconds.from_now, task.run_after, 5

    task.update_columns(attempts: 40)
    task.retry_later!(RuntimeError.new("boom"))
    assert_in_delta 15.minutes.from_now, task.run_after, 5
    assert_equal "RuntimeError: boom", task.last_error
  end

  test "last_deleted_at sees exact and prefix deletes" do
    exact = enqueue("primary", :delete, "invoices/1.pdf")
    exact.update_columns(state: ActiveStorageReplicated::Task.states[:done], created_at: 2.hours.ago)
    prefix = enqueue("primary", :delete_prefixed, "variants/abc/")
    prefix.update_columns(state: ActiveStorageReplicated::Task.states[:done], created_at: 1.hour.ago)

    assert_in_delta 2.hours.ago, ActiveStorageReplicated::Task.last_deleted_at(service_name: :replicated_test, key: "invoices/1.pdf"), 5
    assert_in_delta 1.hour.ago, ActiveStorageReplicated::Task.last_deleted_at(service_name: :replicated_test, key: "variants/abc/1"), 5
    assert_nil ActiveStorageReplicated::Task.last_deleted_at(service_name: :replicated_test, key: "variants_abc_1")
  end
end
