require "test_helper"

class ReplicatedServiceTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  test "needs at least one target" do
    assert_raises(ArgumentError) do
      ActiveStorage::Service::ReplicatedService.new(targets: [])
    end
  end

  test "upload writes to every target and leaves nothing to do" do
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))

    assert_equal "pdf", read_back(@primary, "invoices/1.pdf")
    assert_equal "pdf", read_back(@replica, "invoices/1.pdf")
    assert_equal 0, ActiveStorageReplicated::Task.count
  end

  test "a plain string is uploaded as the content" do
    @service.upload("notes/1.txt", "plain")

    assert_equal "plain", read_back(@primary, "notes/1.txt")
    assert_equal "plain", read_back(@replica, "notes/1.txt")
  end

  test "upload succeeds when one target is down and queues a copy for it" do
    @replica.down = true

    @service.upload("invoices/1.pdf", StringIO.new("pdf"))

    assert_equal "pdf", read_back(@primary, "invoices/1.pdf")
    task = ActiveStorageReplicated::Task.sole
    assert_equal [ "replicated_test", "replica", "invoices/1.pdf" ], [ task.service_name, task.target, task.key ]
    assert_predicate task, :operation_copy?
    assert_predicate task, :pending?
  end

  test "upload succeeds on the replica when the primary is down" do
    @primary.down = true

    @service.upload("invoices/1.pdf", StringIO.new("pdf"))

    assert_equal "pdf", read_back(@replica, "invoices/1.pdf")
    assert_equal "primary", ActiveStorageReplicated::Task.sole.target
  end

  test "upload raises when every target is down" do
    @primary.down = true
    @replica.down = true

    assert_raises(Errno::ECONNREFUSED) do
      @service.upload("invoices/1.pdf", StringIO.new("pdf"))
    end
  end

  test "a target that failed three times in a row is skipped instead of called" do
    @replica.down = true
    3.times { |i| @service.upload("files/#{i}", StringIO.new("x")) }
    assert_equal 3, @replica.calls[:upload]

    @service.upload("files/3", StringIO.new("x"))

    assert_equal 3, @replica.calls[:upload]
    assert_equal 4, ActiveStorageReplicated::Task.pending.operation_copy.where(target: "replica").count
  end

  test "a direct write supersedes an older pending delete for the same target" do
    old_delete = enqueue("replica", :delete, "invoices/1.pdf")

    @service.upload("invoices/1.pdf", StringIO.new("new"))

    assert_predicate old_delete.reload, :skipped?
  end

  test "download falls over to the next target on an error" do
    store(@replica, "statements/1.csv", "rows")
    @primary.down = true

    assert_equal "rows", @service.download("statements/1.csv")
  end

  test "a plain not found costs one storage request" do
    assert_raises(ActiveStorage::FileNotFoundError) do
      @service.download("missing.csv")
    end

    assert_equal 1, @primary.calls[:download]
    assert_equal 0, @replica.calls[:download]
  end

  test "not found on a target that still waits for its copy is read from the next target" do
    store(@replica, "statements/1.csv", "rows")
    enqueue("primary", :copy, "statements/1.csv")

    assert_equal "rows", @service.download("statements/1.csv")
  end

  test "streaming download falls over before the first chunk" do
    store(@replica, "statements/1.csv", "rows")
    @primary.down = true
    chunks = []

    @service.download("statements/1.csv") { |chunk| chunks << chunk }

    assert_equal [ "rows" ], chunks
  end

  test "streaming download does not fall over after a chunk was handed out" do
    store(@primary, "statements/1.csv", "rows")
    store(@replica, "statements/1.csv", "rows")
    chunks = []

    assert_raises(RuntimeError) do
      @service.download("statements/1.csv") do |chunk|
        chunks << chunk
        raise "caller failed"
      end
    end

    assert_equal [ "rows" ], chunks
    assert_equal 0, @replica.calls[:download]
  end

  test "download_chunk and exist? fall over too" do
    store(@replica, "statements/1.csv", "rows")
    @primary.down = true

    assert_equal "ow", @service.download_chunk("statements/1.csv", 1..2)
    assert @service.exist?("statements/1.csv")
  end

  test "a not found after another endpoint failed is not trusted" do
    @primary.down = true

    assert_raises(Errno::ECONNREFUSED) { @service.exist?("missing.csv") }
    assert_raises(Errno::ECONNREFUSED) { @service.download("missing.csv") }
  end

  test "a not found with every endpoint answering is final" do
    assert_not @service.exist?("missing.csv")
  end

  test "delete removes the file everywhere and keeps finished tasks as delete records" do
    store(@primary, "flows/1/a.pdf", "a")
    store(@replica, "flows/1/a.pdf", "a")

    @service.delete("flows/1/a.pdf")

    assert_not stored?(@primary, "flows/1/a.pdf")
    assert_not stored?(@replica, "flows/1/a.pdf")
    assert_equal [ "done", "done" ], ActiveStorageReplicated::Task.order(:id).pluck(:state)
    assert_equal [ nil, nil ], ActiveStorageReplicated::Task.pluck(:claimed_until)
  end

  test "delete never raises and leaves an unclaimed pending task for a down target" do
    store(@primary, "flows/1/a.pdf", "a")
    store(@replica, "flows/1/a.pdf", "a")
    @replica.down = true

    assert_nothing_raised { @service.delete("flows/1/a.pdf") }

    assert_not stored?(@primary, "flows/1/a.pdf")
    pending = ActiveStorageReplicated::Task.pending.sole
    assert_equal "replica", pending.target
    assert_equal 1, pending.attempts
    assert_nil pending.claimed_until
    assert_predicate pending, :operation_delete?
  end

  test "delete skips a target that is marked down without calling it" do
    @replica.down = true
    3.times { |i| @service.upload("files/#{i}", StringIO.new("x")) }

    @service.delete("files/0")

    assert_equal 0, @replica.calls[:delete]
    task = ActiveStorageReplicated::Task.pending.operation_delete.sole
    assert_nil task.claimed_until
  end

  test "delete supersedes an older pending copy" do
    old_copy = enqueue("replica", :copy, "flows/1/a.pdf")

    @service.delete("flows/1/a.pdf")

    assert_predicate old_copy.reload, :skipped?
  end

  test "delete_prefixed and delete_many reach every target" do
    store(@primary, "variants/abc/1", "a")
    store(@replica, "variants/abc/1", "a")
    store(@primary, "a", "a")
    store(@replica, "b", "b")

    @service.delete_prefixed("variants/abc/")
    @service.delete_many([ "a", "b" ])

    assert_not stored?(@primary, "variants/abc/1")
    assert_not stored?(@replica, "variants/abc/1")
    assert_not stored?(@primary, "a")
    assert_not stored?(@replica, "b")
  end

  test "links point at a target that has the file" do
    enqueue("primary", :copy, "invoices/1.pdf")

    assert_equal @replica, @service.send(:url_target, "invoices/1.pdf")
    assert_equal @primary, @service.send(:url_target, "invoices/2.pdf")
  end

  test "reads do not spend the trial call of a target they never call" do
    store(@primary, "a", "a")
    @replica.down = true
    3.times { |i| @service.upload("files/#{i}", StringIO.new("x")) }
    down_until = ActiveStorageReplicated.health.instance_variable_get(:@down_until)
    down_until["replica"] = 0

    @service.download("a")

    assert_equal 0, down_until["replica"]
    assert ActiveStorageReplicated.health.available?("replica")
  end
end
