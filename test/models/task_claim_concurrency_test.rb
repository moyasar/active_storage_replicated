require "test_helper"

# Runs without a wrapping transaction so a second database session can see and lock the rows.
class TaskClaimConcurrencyTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  teardown do
    ActiveStorageReplicated::Task.delete_all
  end

  test "claim skips rows another session has locked" do
    locked = enqueue("a")
    free = enqueue("b")
    holding = Queue.new
    release = Queue.new

    holder = Thread.new do
      ActiveStorageReplicated::Task.connection_pool.with_connection do |connection|
        connection.transaction do
          connection.execute("SELECT id FROM active_storage_replicated_tasks WHERE id = #{locked.id} FOR UPDATE")
          holding << connection.select_value("SELECT pg_backend_pid()")
          release.pop
        end
      end
    end

    holder_pid = holding.pop
    assert_not_equal holder_pid, ActiveStorageReplicated::Task.connection.select_value("SELECT pg_backend_pid()")
    assert_equal [ free.id ], ActiveStorageReplicated::Task.claim(owner: "w").map(&:id)
  ensure
    release << true
    holder&.join
  end

  private

  def enqueue(key)
    ActiveStorageReplicated::Task.enqueue!(service_name: :replicated_test, target: "replica", operation: :copy, key: key)
  end
end
