require "test_helper"

class SchedulerTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service(name: :replicated)
    @original_audit_at = ActiveStorageReplicated.config.audit_at
    ActiveStorageReplicated.config.audit_at = "03:30"
  end

  teardown do
    ActiveStorageReplicated.config.audit_at = @original_audit_at
  end

  test "the daily audit waits for its time" do
    assert_empty ActiveStorageReplicated::Scheduler.run_audit_if_due(now: Time.zone.parse("2026-10-31 03:29"), services: [ @service ])

    assert_equal 0, ActiveStorageReplicated::AuditRun.count
  end

  test "the daily audit runs once per service per day" do
    first = run_at("2026-10-31 03:31")
    second = run_at("2026-10-31 09:00")
    next_day = run_at("2026-11-01 03:31")

    assert_equal 1, first.size
    assert_empty second
    assert_equal 1, next_day.size

    run = ActiveStorageReplicated::AuditRun.order(:run_on).first
    assert_equal "replicated", run.service_name
    assert_not_nil run.finished_at
    assert_equal 0, run.summary["checked"]
  end

  test "no audit when the time is not set" do
    ActiveStorageReplicated.config.audit_at = nil

    assert_empty ActiveStorageReplicated::Scheduler.run_audit_if_due(services: [ @service ])
  end

  private

  def run_at(time)
    ActiveStorageReplicated::Scheduler.run_audit_if_due(now: Time.zone.parse(time), services: [ @service ])
  end

  test "claims one service's audit at a time, so waiting audits never look abandoned" do
    other = build_replicated_service(name: :replicated_other)

    ActiveStorageReplicated::Scheduler.send(:due_audits, [ @service, other ], now: Time.zone.parse("2026-10-31 04:00"))

    assert_equal 1, ActiveStorageReplicated::AuditRun.count
  end

  test "running due audits by hand runs every service" do
    other = build_replicated_service(name: :replicated_other)

    runs = ActiveStorageReplicated::Scheduler.run_audit_if_due(now: Time.zone.parse("2026-10-31 04:00"), services: [ @service, other ])

    assert_equal 2, runs.size
    assert runs.all?(&:finished_at)
  end
end
