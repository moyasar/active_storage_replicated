require "test_helper"
require "generators/active_storage_replicated/install/install_generator"

class InstallGeneratorDatabaseTest < Rails::Generators::TestCase
  tests ActiveStorageReplicated::Generators::InstallGenerator
  destination File.expand_path("../../tmp/generator_database_test", __dir__)
  setup :prepare_destination

  test "--database puts the migration where that database keeps its migrations" do
    config = ActiveRecord::DatabaseConfigurations::HashConfig.new("test", "storage", { "adapter" => "postgresql", "migrations_paths" => "db/storage_migrate" })
    configurations = ActiveRecord::Base.configurations
    ActiveRecord::Base.configurations = ActiveRecord::DatabaseConfigurations.new(configurations.configurations + [ config ])

    run_generator [ "--database", "storage" ]

    assert_migration "db/storage_migrate/create_active_storage_replicated_tables.rb"
    assert_file "config/initializers/active_storage_replicated.rb", /^  config.database = :storage$/
  ensure
    ActiveRecord::Base.configurations = configurations
  end
end
