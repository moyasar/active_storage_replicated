require "puma/plugin"

# Starts the background thread in every Puma worker, or in the single process when Puma runs without workers.
# The thread waits for the Rails app to finish booting, so it works with and without preload_app!.
#
# The plugin defines no `config` hook on purpose: Puma removes the first option of the config file when a
# plugin defines one. Instead, `start` runs in the Puma master before any worker is forked, when the final
# worker count is known (config file, command line or WEB_CONCURRENCY), and adds the worker boot hook then.
Puma::Plugin.create do
  def start(launcher)
    if launcher.options[:workers].to_i.zero?
      start_scheduler
    elsif !add_worker_boot_hook(launcher)
      launcher.log_writer.log(
        "active_storage_replicated: this Puma version keeps its hooks differently, so the background thread " \
        "cannot start in workers. Add `before_worker_boot { ActiveStorageReplicated::Scheduler.start }` to the config.",
      )
    end
  end

  private

  def add_worker_boot_hook(launcher)
    options = launcher.options
    unless options.respond_to?(:default_options) && options.default_options.is_a?(Hash)
      return false
    end

    plugin = self
    block = ->(*) { plugin.send(:start_scheduler) }
    hooks = Array(options.default_options[:before_worker_boot])
    hooks << (hook_hashes? ? { block: block, cluster_only: true } : block)
    options.default_options[:before_worker_boot] = hooks
    true
  end

  # Puma 7 and later keep each hook as a hash with its block; Puma 6 keeps the block itself.
  def hook_hashes?
    Gem::Version.new(Puma::Const::PUMA_VERSION) >= Gem::Version.new("7.0.0")
  end

  def start_scheduler
    Thread.new do
      Thread.current.name = "active-storage-replicated"

      until rails_ready?
        sleep(1)
      end

      ActiveStorageReplicated::Scheduler.run_loop
    end
  end

  def rails_ready?
    unless defined?(::Rails)
      return false
    end

    application = ::Rails.application
    !application.nil? && application.initialized?
  end
end
