require "test_helper"

class SingleTargetTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service(names: %w[primary])
    @primary = @service.targets.sole
    @original_audit_at = ActiveStorageReplicated.config.audit_at
  end

  teardown do
    ActiveStorageReplicated.config.audit_at = @original_audit_at
  end

  test "upload, read, link and delete go to the one endpoint and leave nothing pending" do
    @service.upload("invoices/1.pdf", StringIO.new("pdf"), checksum: OpenSSL::Digest::MD5.base64digest("pdf"))

    assert_equal "pdf", read_back(@primary, "invoices/1.pdf")
    assert_equal "pdf", @service.download("invoices/1.pdf")
    assert_equal "df", @service.download_chunk("invoices/1.pdf", 1..2)
    assert @service.exist?("invoices/1.pdf")
    assert_equal 0, ActiveStorageReplicated::Task.count

    @service.delete("invoices/1.pdf")

    assert_not stored?(@primary, "invoices/1.pdf")
    assert_not @service.exist?("invoices/1.pdf")
    assert_equal 0, ActiveStorageReplicated::Task.pending.count
  end

  test "a wrong checksum fails the upload and leaves nothing pending" do
    assert_raises(ActiveStorage::IntegrityError) do
      @service.upload("invoices/1.pdf", StringIO.new("pdf"), checksum: OpenSSL::Digest::MD5.base64digest("other"))
    end

    assert_equal 0, ActiveStorageReplicated::Task.count
  end

  test "an upload interrupted while writing leaves nothing to settle" do
    original = @primary.method(:upload)
    @primary.define_singleton_method(:upload) do |*args, **options|
      original.call(*args, **options)
      raise Class.new(Exception), "request timeout"
    end

    assert_raises(Exception) { @service.upload("invoices/1.pdf", StringIO.new("pdf")) }

    assert_equal "pdf", read_back(@primary, "invoices/1.pdf")
    assert_equal 0, ActiveStorageReplicated::Task.count
  end

  test "a delete whose result could not be recorded queues no correction" do
    store(@primary, "invoices/1.pdf", "pdf")
    task = enqueue("primary", :delete, "invoices/1.pdf")
    runner = ActiveStorageReplicated::Runner.new([ @service ])
    claimed = ActiveStorageReplicated::Task.claim(owner: runner.instance_variable_get(:@owner)).sole
    original = @primary.method(:delete)
    @primary.define_singleton_method(:delete) do |key|
      original.call(key)
      ActiveStorageReplicated::Task.where(id: task.id).update_all(state: ActiveStorageReplicated::Task.states[:skipped])
    end

    assert_equal :busy, runner.send(:perform, claimed)
    assert_not stored?(@primary, "invoices/1.pdf")
    assert_equal 0, ActiveStorageReplicated::Task.pending.count
  end

  test "a delete that fails is retried by the catch-up, and the file reads as deleted meanwhile" do
    store(@primary, "invoices/1.pdf", "pdf")
    @primary.down = true

    @service.delete("invoices/1.pdf")

    assert_not @service.exist?("invoices/1.pdf")
    assert_predicate ActiveStorageReplicated::Task.sole, :pending?

    @primary.down = false
    ActiveStorageReplicated.health.reset!
    travel(1.minute) { ActiveStorageReplicated::Runner.run_once([ @service ]) }

    assert_not stored?(@primary, "invoices/1.pdf")
    assert_predicate ActiveStorageReplicated::Task.sole, :done?
  end

  test "an upload after a failed delete of the same key replaces the delete" do
    store(@primary, "invoices/1.pdf", "old")
    @primary.down = true
    @service.delete("invoices/1.pdf")
    @primary.down = false

    @service.upload("invoices/1.pdf", StringIO.new("new"))
    travel(1.minute) { ActiveStorageReplicated::Runner.run_once([ @service ]) }

    assert_equal "new", @service.download("invoices/1.pdf")
    assert_equal 0, ActiveStorageReplicated::Task.pending.count
  end

  test "streamed downloads, streamed uploads and prefix deletes work" do
    @service.stream_upload("exports/1.csv") { |io| io.write("a,b") }
    @service.upload("variants/k/1", StringIO.new("v"))

    chunks = []
    @service.download("exports/1.csv") { |chunk| chunks << chunk }
    @service.delete_prefixed("variants/k/")

    assert_equal "a,b", chunks.join
    assert_not stored?(@primary, "variants/k/1")
    assert_equal 0, ActiveStorageReplicated::Task.pending.count
  end

  test "a missing file raises not found" do
    assert_raises(ActiveStorage::FileNotFoundError) { @service.download("missing") }
  end

  test "an endpoint error reaches the caller even after the endpoint is marked down" do
    @primary.down = true

    4.times do
      assert_raises(Errno::ECONNREFUSED) { @service.download("anything") }
    end

    @primary.down = false
    @service.upload("after", StringIO.new("ok"))

    assert_equal "ok", @service.download("after")
  end

  test "the catch-up has nothing to do and the audit is skipped" do
    ActiveStorageReplicated.config.audit_at = "03:30"
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_empty ActiveStorageReplicated::Scheduler.run_audit_if_due(now: Time.zone.parse("2026-10-31 03:31"), services: [ @service ])
    assert_equal 0, ActiveStorageReplicated::AuditRun.count
    assert_equal({ checked: 0, copies: 0, deletes: 0, skipped: 0 }, ActiveStorageReplicated::Audit.new(@service).run)
  end

  test "a direct upload goes to the endpoint with nothing queued" do
    ActiveStorage::Current.url_options = { host: "www.example.com" }
    url = @service.url_for_direct_upload("uploads/1", expires_in: 5.minutes, content_type: "text/plain", content_length: 1, checksum: "x")

    assert_not_nil url
    assert_equal 0, ActiveStorageReplicated::Task.count
  end

  test "adding a second endpoint later copies the files it lacks" do
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))
    File.utime(3.hours.ago.to_time, 3.hours.ago.to_time, @primary.path_for("invoices/1.pdf"))
    replica_root = Dir.mktmpdir("replica")
    @storage_roots << replica_root
    replica = ActiveStorage::Service::FlakyDiskService.new(root: replica_root).tap { |target| target.name = "replica" }
    grown = ActiveStorage::Service::ReplicatedService.new(targets: [ @primary, replica ]).tap { |service| service.name = @service.name }

    assert_equal 1, ActiveStorageReplicated::Audit.new(grown).run[:copies]
    ActiveStorageReplicated::Runner.run_once([ grown ])

    assert_equal "pdf", read_back(replica, "invoices/1.pdf")
  end
end
