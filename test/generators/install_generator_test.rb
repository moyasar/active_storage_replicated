require "test_helper"
require "generators/active_storage_replicated/install/install_generator"

class InstallGeneratorTest < Rails::Generators::TestCase
  tests ActiveStorageReplicated::Generators::InstallGenerator
  destination File.expand_path("../../tmp/generator_test", __dir__)
  setup :prepare_destination

  test "creates the migration and the initializer" do
    run_generator

    assert_migration "db/migrate/create_active_storage_replicated_tables.rb" do |migration|
      assert_match(/create_table :active_storage_replicated_tasks/, migration)
      assert_match(/create_table :active_storage_replicated_audits/, migration)
      assert_match(/t.datetime :claimed_until/, migration)
    end

    assert_file "config/initializers/active_storage_replicated.rb", /ActiveStorageReplicated.configure/
  end
end
