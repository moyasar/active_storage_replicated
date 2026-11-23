require "test_helper"

class SharedBucketTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @first_app = build_replicated_service
    @primary, @replica = @first_app.targets
    store_old(@primary, "invoices/1.pdf", "pdf")
    store_old(@replica, "invoices/1.pdf", "pdf")
  end

  test "an app sharing the service name and the task table finishes the other app's delete" do
    second_app = same_bucket_service(name: @first_app.name)
    delete_while_replica_is_down

    assert_equal 0, audit(second_app)[:copies]
    assert stored?(@replica, "invoices/1.pdf")

    travel 1.minute do
      ActiveStorageReplicated::Runner.run_once([ second_app ])
    end

    assert_not stored?(@primary, "invoices/1.pdf")
    assert_not stored?(@replica, "invoices/1.pdf")
    assert_equal 0, audit(second_app)[:checked]
  end

  test "an app sharing the service name and the task table does not copy back a finished delete" do
    second_app = same_bucket_service(name: @first_app.name)
    @first_app.delete("invoices/1.pdf")
    store_old(@replica, "invoices/1.pdf", "pdf", age: 3.hours)
    ActiveStorageReplicated::Task.where(key: "invoices/1.pdf").update_all(created_at: 2.hours.ago)

    summary = audit(second_app)

    assert_equal 1, summary[:deletes]
    assert_equal 0, ActiveStorageReplicated::Task.pending.operation_copy.count
  end

  test "under another service name the other app's delete is invisible, so the file is copied back" do
    second_app = same_bucket_service(name: :other_app)
    delete_while_replica_is_down

    assert_equal 1, audit(second_app)[:copies]
    assert_equal [ "other_app", "primary" ], ActiveStorageReplicated::Task.pending.operation_copy.sole.then { |task| [ task.service_name, task.target ] }
  end

  private

  def same_bucket_service(name:)
    targets = @first_app.targets.map do |target|
      ActiveStorage::Service::FlakyDiskService.new(root: target.root).tap { |copy| copy.name = target.name }
    end

    ActiveStorage::Service::ReplicatedService.new(targets: targets).tap { |service| service.name = name }
  end

  def delete_while_replica_is_down
    @replica.down = true
    @first_app.delete("invoices/1.pdf")
    @replica.down = false
    ActiveStorageReplicated.health.reset!

    assert_not stored?(@primary, "invoices/1.pdf")
    assert stored?(@replica, "invoices/1.pdf")
    assert_equal "replica", ActiveStorageReplicated::Task.pending.operation_delete.sole.target
  end

  def audit(service)
    ActiveStorageReplicated::Audit.new(service).run
  end

  def store_old(target, key, content, age: 2.hours)
    store(target, key, content)
    time = age.ago.to_time
    File.utime(time, time, target.path_for(key))
  end
end
