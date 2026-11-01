require "test_helper"

class SyncCopiesTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = build_replicated_service(names: %w[a b c])
    @a, @b, @c = @service.targets
    @original = ActiveStorageReplicated.config.sync_copies
  end

  teardown do
    ActiveStorageReplicated.config.sync_copies = @original
  end

  test "defaults to every healthy endpoint" do
    assert_equal :all, ActiveStorageReplicated::Configuration.new.sync_copies

    @service.upload("f", StringIO.new("x"))

    assert [ @a, @b, @c ].all? { |target| stored?(target, "f") }
    assert_equal 0, ActiveStorageReplicated::Task.count
  end

  test "1 returns after the first endpoint and queues copies for the rest" do
    ActiveStorageReplicated.config.sync_copies = 1

    @service.upload("f", StringIO.new("x"))

    assert stored?(@a, "f")
    assert_not stored?(@b, "f")
    assert_not stored?(@c, "f")
    assert_equal %w[b c], ActiveStorageReplicated::Task.pending.operation_copy.order(:target).pluck(:target)

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert stored?(@b, "f")
    assert stored?(@c, "f")
  end

  test "1 keeps trying in order until one endpoint accepts the file" do
    ActiveStorageReplicated.config.sync_copies = 1
    @a.down = true
    @b.down = true

    @service.upload("f", StringIO.new("x"))

    assert stored?(@c, "f")
    assert_equal %w[a b], ActiveStorageReplicated::Task.pending.operation_copy.order(:target).pluck(:target)
  end

  test "2 waits for two endpoints" do
    ActiveStorageReplicated.config.sync_copies = 2

    @service.upload("f", StringIO.new("x"))

    assert stored?(@a, "f")
    assert stored?(@b, "f")
    assert_not stored?(@c, "f")
    assert_equal [ "c" ], ActiveStorageReplicated::Task.pending.pluck(:target)
  end

  test "fewer healthy endpoints than wanted still succeeds" do
    ActiveStorageReplicated.config.sync_copies = 2
    @b.down = true
    @c.down = true

    @service.upload("f", StringIO.new("x"))

    assert stored?(@a, "f")
    assert_equal %w[b c], ActiveStorageReplicated::Task.pending.order(:target).pluck(:target)
  end

  test "a number above the endpoint count acts like :all" do
    ActiveStorageReplicated.config.sync_copies = 10

    @service.upload("f", StringIO.new("x"))

    assert [ @a, @b, @c ].all? { |target| stored?(target, "f") }
  end

  test "still fails when no endpoint accepts the file" do
    ActiveStorageReplicated.config.sync_copies = 1
    [ @a, @b, @c ].each { |target| target.down = true }

    assert_raises(Errno::ECONNREFUSED) { @service.upload("f", StringIO.new("x")) }
  end

  test "rejects values that are not :all or a positive integer" do
    config = ActiveStorageReplicated::Configuration.new

    [ 0, -1, "1", :some, nil ].each do |value|
      assert_raises(ArgumentError) { config.sync_copies = value }
    end
  end
end
