require "test_helper"

class AuditRunTest < ActiveSupport::TestCase
  test "only one process starts each day's audit" do
    first = ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current)

    assert first
    assert_nil ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current)
  end

  test "a run whose process died is taken over once its heartbeat is stale" do
    run = ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current)
    run.update_columns(heartbeat_at: 11.minutes.ago)

    taken = ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current)

    assert_equal run.id, taken.id
    assert_equal 2, taken.attempts, "takeovers count, so an audit that keeps killing its worker stops"
  end

  test "a live run is not taken over" do
    ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current).heartbeat!

    assert_nil ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current)
  end

  test "a failed run is retried after an hour, a few times at most" do
    run = ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current)
    run.fail!(RuntimeError.new("listing failed"))
    assert_nil ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current)

    run.update_columns(finished_at: 2.hours.ago)
    assert ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current)

    run.reload.update_columns(error: "again", finished_at: 2.hours.ago, attempts: ActiveStorageReplicated::AuditRun::MAX_ATTEMPTS)
    assert_nil ActiveStorageReplicated::AuditRun.claim(service_name: :s, run_on: Date.current)
  end
end
