require "active_storage_replicated/version"
require "active_storage_replicated/configuration"
require "active_storage_replicated/errors"
require "active_storage_replicated/instrumentation"
require "active_storage_replicated/health"
require "active_storage_replicated/deferred"
require "active_storage_replicated/engine"

module ActiveStorageReplicated
  autoload :Runner, "active_storage_replicated/runner"
  autoload :Audit, "active_storage_replicated/audit"
  autoload :Scheduler, "active_storage_replicated/scheduler"
  autoload :Status, "active_storage_replicated/status"
  autoload :Redirect, "active_storage_replicated/redirect"

  @config = Configuration.new
  @health = Health.new

  class << self
    attr_reader :config, :health

    def configure
      yield config
    end

    def logger
      config.logger || Rails.logger
    end

    # Whether the caller is inside a transaction of its own. Rails test fixtures wrap each test in a transaction
    # the app cannot join; that one does not count.
    def in_app_transaction?
      Task.with_connection { |connection| connection.transaction_open? && connection.current_transaction.joinable? }
    end

    # Runs the block on the writing role, so apps that send GET requests to a read replica still read and write
    # the to-do table on the primary.
    def with_writing(&block)
      klass = Record.connection_class? ? Record : ActiveRecord::Base
      klass.connected_to(role: ActiveRecord.writing_role, &block)
    end

    # Every storage service in storage.yml whose type is Replicated.
    def services
      registry = ActiveStorage::Blob.services
      configurations = Rails.application.config.active_storage.service_configurations || {}

      configurations
        .select { |_name, service_config| service_config.with_indifferent_access[:service].to_s == "Replicated" }
        .keys
        .map { |name| registry.fetch(name) }
    end

    # Pending work and the last audit for each replicated service, for dashboards and health checks.
    def status
      Status.new(services).to_h
    end

    def redirect_url(key, expires_in:, service: ActiveStorage::Blob.service, **options)
      Redirect.url(key, expires_in: expires_in, service: service, **options)
    end
  end
end
