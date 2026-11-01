require "test_helper"

class StreamUploadTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
  end

  test "streams to the first healthy target and queues a copy for the other" do
    key = @service.stream_upload("exports/1.csv") { |io| io.write("a,b\n" * 3) }

    assert_equal "exports/1.csv", key
    assert_equal "a,b\n" * 3, read_back(@primary, "exports/1.csv")
    assert_not stored?(@replica, "exports/1.csv")
    assert_equal [ "replica", "exports/1.csv" ], ActiveStorageReplicated::Task.pending.pluck(:target, :key).sole

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_equal "a,b\n" * 3, read_back(@replica, "exports/1.csv")
  end

  test "streams to the replica while the primary is marked down" do
    @primary.down = true
    3.times { |i| @service.upload("x/#{i}", StringIO.new("x")) }

    @service.stream_upload("exports/1.csv") { |io| io.write("a,b") }

    assert_equal "a,b", read_back(@replica, "exports/1.csv")
    assert ActiveStorageReplicated::Task.pending.exists?(target: "primary", key: "exports/1.csv")
  end

  test "an error from the caller's block does not count against the endpoint" do
    3.times do
      assert_raises(RuntimeError) do
        @service.stream_upload("exports/1.csv") { |_io| raise "export failed" }
      end
    end

    assert_not ActiveStorageReplicated.health.down?("primary")
    assert_equal 0, ActiveStorageReplicated::Task.count
  end

  test "an endpoint error counts against the endpoint and raises" do
    @primary.down = true

    assert_raises(Errno::ECONNREFUSED) do
      @service.stream_upload("exports/1.csv") { |io| io.write("a") }
    end

    assert_equal 1, ActiveStorageReplicated.health.instance_variable_get(:@failures)["primary"]
  end

  test "needs a block" do
    assert_raises(ArgumentError) { @service.stream_upload("exports/1.csv") }
  end
end
