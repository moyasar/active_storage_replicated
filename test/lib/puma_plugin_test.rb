require "test_helper"
require "puma"
require "puma/configuration"
require "puma/log_writer"
require "puma/plugin/active_storage_replicated"

# Loads real Puma config files through Puma's own loader, then runs the plugin's start step.
class PumaPluginTest < ActiveSupport::TestCase
  FakeLauncher = Struct.new(:options, :log_writer)

  setup do
    @dir = Dir.mktmpdir("puma-config")
  end

  teardown do
    FileUtils.rm_rf(@dir)
  end

  test "registers under its file name" do
    assert Puma::Plugins.find("active_storage_replicated")
  end

  test "does not remove any option from the config file" do
    config = load_config(<<~RUBY)
      port 3123
      workers 4
      threads 2, 5
      plugin :active_storage_replicated
    RUBY

    assert_equal 4, config.options[:workers]
    assert_equal 2, config.options[:min_threads]
    assert config.options[:binds].any? { |bind| bind.include?("3123") }
  end

  test "adds the worker boot hook when workers come from the config file" do
    config = load_config("workers 2\nplugin :active_storage_replicated\n")

    started = start_plugin(config)

    assert_equal 1, config.options.all_of(:before_worker_boot).size
    assert_equal 0, started.size
  end

  test "adds the worker boot hook when workers come from the command line" do
    config = load_config("plugin :active_storage_replicated\n", user_options: { workers: 3 })

    start_plugin(config)

    assert_equal 1, config.options.all_of(:before_worker_boot).size
  end

  test "adds the worker boot hook when workers come from WEB_CONCURRENCY" do
    config = load_config("plugin :active_storage_replicated\n", env: { "WEB_CONCURRENCY" => "3" })

    start_plugin(config)

    assert_equal 3, config.options[:workers]
    assert_equal 1, config.options.all_of(:before_worker_boot).size
  end

  test "the worker boot hook starts the background thread in the worker" do
    config = load_config("workers 2\nplugin :active_storage_replicated\n")
    started = start_plugin(config)

    config.run_hooks(:before_worker_boot, 0, Puma::LogWriter.null, {})

    assert_equal 1, started.size
  end

  test "starts the background thread right away without workers" do
    config = load_config("plugin :active_storage_replicated\n")

    started = start_plugin(config)

    assert_equal 1, started.size
    assert_equal 0, config.options.all_of(:before_worker_boot).size
  end

  test "sees the booted Rails app" do
    plugin = Puma::Plugins.find("active_storage_replicated").new

    assert plugin.send(:rails_ready?)
  end

  private

  def load_config(contents, user_options: {}, env: {})
    path = File.join(@dir, "puma.rb")
    File.write(path, contents)
    config = Puma::Configuration.new(user_options.merge(config_files: [ path ]), {}, env)
    config.load
    config.clamp if config.respond_to?(:clamp)
    config
  end

  # Runs the plugin's start step with its thread start recorded instead of performed.
  def start_plugin(config)
    started = []
    config.plugins.instance_variable_get(:@instances).each do |plugin|
      plugin.define_singleton_method(:start_scheduler) { started << true }
    end

    config.plugins.fire_starts(FakeLauncher.new(config.options, Puma::LogWriter.null))
    started
  end
end
