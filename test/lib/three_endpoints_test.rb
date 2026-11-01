require "test_helper"

class ThreeEndpointsTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service(names: %w[a b c])
    @a, @b, @c = @service.targets
  end

  test "a copy waits instead of being skipped while the only endpoint holding the file is down" do
    store(@a, "doc.pdf", "x")
    task = enqueue("b", :copy, "doc.pdf")
    @a.down = true
    3.times { ActiveStorageReplicated.health.failure("a") }

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_predicate task.reload, :pending?

    @a.down = false
    ActiveStorageReplicated.health.reset!
    task.update!(run_after: Time.current)
    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_predicate task.reload, :done?
    assert stored?(@b, "doc.pdf")
  end

  test "a copy is skipped only when every other endpoint answered that the file is gone" do
    task = enqueue("b", :copy, "gone")

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_predicate task.reload, :skipped?
  end

  test "a copy with a source uses only that endpoint" do
    store(@c, "doc.pdf", "from-c")
    store(@a, "doc.pdf", "from-a")
    ActiveStorageReplicated::Task.enqueue!(service_name: :replicated_test, target: "b", operation: :copy, key: "doc.pdf", source: "c")

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_equal "from-c", read_back(@b, "doc.pdf")
  end

  test "the runner stops at its time budget and gives the rest back" do
    10.times { |i| store(@a, "f#{i}", "x") }
    10.times { |i| enqueue("b", :copy, "f#{i}") }
    original = ActiveStorageReplicated.config.time_budget
    ActiveStorageReplicated.config.time_budget = 1
    slow = @b.method(:upload)
    @b.define_singleton_method(:upload) do |*args, **options|
      sleep 0.3
      slow.call(*args, **options)
    end

    ActiveStorageReplicated::Runner.run_once([ @service ])

    done = ActiveStorageReplicated::Task.done.count
    assert_operator done, :<, 10
    assert_equal 0, ActiveStorageReplicated::Task.pending.where.not(claimed_until: nil).count
  ensure
    ActiveStorageReplicated.config.time_budget = original
  end

  test "copies wait without any writes while every source is down, and the run stops early" do
    50.times { |i| enqueue("b", :copy, "f#{i}") }
    3.times { ActiveStorageReplicated.health.failure("a") }
    3.times { ActiveStorageReplicated.health.failure("c") }

    writes = 0
    subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
      if payload[:sql].start_with?("UPDATE") && payload[:sql].include?("attempts")
        writes += 1
      end
    end
    summary = ActiveStorageReplicated::Runner.run_once([ @service ])
    ActiveSupport::Notifications.unsubscribe(subscriber)

    assert_equal 0, writes
    assert_equal 0, summary.values_at(:done, :failed, :waiting).sum
    assert_equal 0, ActiveStorageReplicated::Task.pending.where.not(claimed_by: nil).count
  end

  test "a copy never takes a stale version from an endpoint that is itself waiting for a copy" do
    store(@a, "inv.pdf", "v2")
    store(@b, "inv.pdf", "v1")
    store(@c, "inv.pdf", "v1")
    to_b = enqueue("b", :copy, "inv.pdf")
    to_c = enqueue("c", :copy, "inv.pdf")
    3.times { ActiveStorageReplicated.health.failure("a") }

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_predicate to_b.reload, :pending?
    assert_predicate to_c.reload, :pending?
    assert_equal "v1", read_back(@b, "inv.pdf")

    ActiveStorageReplicated.health.reset!
    ActiveStorageReplicated::Task.update_all(run_after: Time.current)
    2.times { ActiveStorageReplicated::Runner.run_once([ @service ]) }

    assert_equal "v2", read_back(@b, "inv.pdf")
    assert_equal "v2", read_back(@c, "inv.pdf")
  end
end
