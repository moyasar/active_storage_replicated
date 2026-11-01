require "test_helper"
require "aws-sdk-s3"

# Fixes from the sixth review.
class Review6Test < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  test "a late folder delete keeps a file written in the same second as the request" do
    store(@replica, "exports/1/report.csv", "new")
    task = enqueue("replica", :delete_prefixed, "exports/1/")
    task.update_columns(created_at: Time.current.change(usec: 0) + 0.5)

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert stored?(@replica, "exports/1/report.csv")
  end

  test "an upload interrupted while writing queues a settle for every endpoint" do
    @replica.define_singleton_method(:upload) { |*, **| raise Class.new(Exception), "request timeout" }

    assert_raises(Exception) { @service.upload("doc.pdf", StringIO.new("x")) }

    settles = ActiveStorageReplicated::Task.pending.where(key: "doc.pdf")
    assert_equal %w[primary replica], settles.order(:target).pluck(:target)
    assert settles.all?(&:reconcile?)
    assert settles.all? { |task| task.run_after > 4.minutes.from_now }
  end

  test "a settle copies whatever an interrupted write left on one endpoint to the others" do
    store(@replica, "doc.pdf", "arrived")
    %w[primary replica].each do |target|
      ActiveStorageReplicated::Task.enqueue!(service_name: :replicated_test, target: target, operation: :copy, key: "doc.pdf",
        source: ActiveStorageReplicated::Task::SOURCE_RECONCILE)
    end

    2.times { ActiveStorageReplicated::Runner.run_once([ @service ]) }

    assert_equal "arrived", read_back(@primary, "doc.pdf")
  end

  test "with three endpoints, a settle deletes a brought-back copy when another endpoint has a pending delete" do
    service = build_replicated_service(names: %w[a b c])
    a, b, _c = service.targets
    store(a, "doc.pdf", "x")
    store(b, "doc.pdf", "x")
    [ a, b ].each { |target| File.utime(2.hours.ago.to_time, 2.hours.ago.to_time, target.path_for("doc.pdf")) }
    enqueue("a", :delete, "doc.pdf").update_columns(created_at: 1.hour.ago, run_after: 1.hour.from_now)
    ActiveStorageReplicated::Task.enqueue!(service_name: :replicated_test, target: "b", operation: :copy, key: "doc.pdf",
      source: ActiveStorageReplicated::Task::SOURCE_RECONCILE)

    ActiveStorageReplicated::Runner.run_once([ service ])

    assert_not stored?(b, "doc.pdf")
  end

  test "a write that fails everywhere puts back rows that were held in memory" do
    ActiveStorageReplicated::Deferred.add([
      ActiveStorageReplicated::Deferred.row(service_name: :replicated_test, target: "replica", operation: :delete, key: "doc.pdf")
    ])
    ActiveStorageReplicated::Task.stub_insert_failure = true
    @primary.down = true
    @replica.down = true

    assert_raises(Errno::ECONNREFUSED) { @service.upload("doc.pdf", StringIO.new("x")) }

    assert_equal 1, ActiveStorageReplicated::Deferred.size
  ensure
    ActiveStorageReplicated::Task.stub_insert_failure = false
  end

  test "refused credentials mark an endpoint down" do
    error = Aws::S3::Errors::Forbidden.new(Seahorse::Client::RequestContext.new(http_response: Seahorse::Client::Http::Response.new(status_code: 403)), "denied")

    assert ActiveStorageReplicated::Errors.endpoint_error?(error)
  end

  test "a lane whose whole backlog is waiting for a retry is skipped with one indexed query" do
    50.times { |i| enqueue("replica", :copy, "f#{i}").update_columns(run_after: 1.hour.from_now) }

    queries = 0
    subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
      queries += 1 if payload[:sql].include?("active_storage_replicated_tasks") && payload[:sql].start_with?("SELECT")
    end
    tasks = ActiveStorageReplicated::Task.claim(owner: "w", lane: { service_name: "replicated_test", target: "replica", operation: "copy" })
    ActiveSupport::Notifications.unsubscribe(subscriber)

    assert_empty tasks
    assert_equal 1, queries
  end
end
