require "test_helper"

class HealthTest < ActiveSupport::TestCase
  setup do
    @health = ActiveStorageReplicated::Health.new
    @clock = 1_000.0
    clock = -> { @clock }
    @health.define_singleton_method(:now) { clock.call }
  end

  test "three failures in a row mark a target down, and one trial is let through after the cool-off" do
    2.times { @health.failure("replica") }
    assert @health.available?("replica")

    @health.failure("replica")
    assert_not @health.available?("replica")

    @clock += 31
    assert @health.available?("replica")
    assert_not @health.available?("replica")

    @health.success("replica")
    assert @health.available?("replica")
    assert_not @health.down?("replica")
  end

  test "a failed trial keeps the target down for another cool-off" do
    3.times { @health.failure("replica") }

    @clock += 31
    assert @health.available?("replica")
    @health.failure("replica")

    assert_not @health.available?("replica")
  end

  test "slow failures far apart still count, and a success resets the count" do
    2.times do
      @health.failure("replica")
      @clock += 40
    end
    @health.success("replica")
    2.times do
      @health.failure("replica")
      @clock += 40
    end
    assert @health.available?("replica")

    @health.failure("replica")
    assert_not @health.available?("replica")
  end

  test "an answer that is not an endpoint error keeps the count but brings a down target back" do
    2.times { @health.failure("replica") }
    @health.failure("replica", ActiveStorage::FileNotFoundError.new)
    assert @health.available?("replica")

    @health.failure("replica")
    assert_not @health.available?("replica")

    @clock += 31
    @health.answered("replica")
    assert_not @health.down?("replica")
    assert @health.available?("replica")
  end

  test "reads the threshold and cool-off from the configuration" do
    config = ActiveStorageReplicated.config
    original = [ config.failure_threshold, config.cool_off ]
    config.failure_threshold = 1
    config.cool_off = 5

    @health.failure("replica")
    assert_not @health.available?("replica")
    @clock += 6
    assert @health.available?("replica")
  ensure
    config.failure_threshold, config.cool_off = original
  end
end
