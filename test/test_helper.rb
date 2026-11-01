# Configure Rails Environment
ENV["RAILS_ENV"] = "test"

require_relative "../test/dummy/config/environment"
ActiveRecord::Migrator.migrations_paths = [ File.expand_path("../test/dummy/db/migrate", __dir__) ]
require "rails/test_help"
require "active_storage/service/disk_service"
require "active_storage/service/replicated_service"

if ActiveSupport::TestCase.respond_to?(:fixture_paths=)
  ActiveSupport::TestCase.fixture_paths = [ File.expand_path("fixtures", __dir__) ]
  ActionDispatch::IntegrationTest.fixture_paths = ActiveSupport::TestCase.fixture_paths
  ActiveSupport::TestCase.file_fixture_path = File.expand_path("fixtures", __dir__) + "/files"
  ActiveSupport::TestCase.fixtures :all
end

module ActiveStorage
  # A Disk service that can be switched off to act like an unreachable endpoint, and counts its calls.
  class Service::FlakyDiskService < Service::DiskService
    attr_accessor :down
    attr_reader :calls

    def initialize(**options)
      super
      @calls = Hash.new(0)
    end

    %i[upload download download_chunk delete delete_prefixed exist?].each do |method_name|
      define_method(method_name) do |*args, **options, &block|
        @calls[method_name] += 1
        if down
          raise Errno::ECONNREFUSED, "endpoint down"
        end

        super(*args, **options, &block)
      end
    end
  end
end

module ReplicationHelpers
  def self.included(base)
    base.setup do
      ActiveStorageReplicated.health.reset!
      ActiveStorageReplicated::Deferred.clear
    end

    base.teardown do
      ActiveStorageReplicated.health.reset!
      ActiveStorageReplicated::Deferred.clear
      Array(@storage_roots).each { |root| FileUtils.rm_rf(root) }
    end
  end

  def build_replicated_service(name: :replicated_test, names: %w[primary replica])
    @storage_roots = names.map { |target_name| Dir.mktmpdir(target_name) }

    targets = names.zip(@storage_roots).map do |target_name, root|
      ActiveStorage::Service::FlakyDiskService.new(root: root).tap { |target| target.name = target_name }
    end

    ActiveStorage::Service::ReplicatedService.new(targets: targets).tap do |service|
      service.name = name
    end
  end

  def store(target, key, content)
    target.upload(key, StringIO.new(content))
  end

  def stored?(target, key)
    File.exist?(target.path_for(key))
  end

  def read_back(target, key)
    File.binread(target.path_for(key))
  end

  def enqueue(target, operation, key, service_name: :replicated_test)
    ActiveStorageReplicated::Task.enqueue!(service_name: service_name, target: target, operation: operation, key: key)
  end
end

# Lets a test make the deferred-row insert fail, as it would while the database is still down.
ActiveStorageReplicated::Task.singleton_class.class_eval do
  attr_accessor :stub_insert_failure

  alias_method :original_insert_all!, :insert_all!

  def insert_all!(*args, **options)
    if stub_insert_failure
      raise ActiveRecord::ConnectionNotEstablished, "database is still down"
    end

    original_insert_all!(*args, **options)
  end
end
