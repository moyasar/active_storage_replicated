require "rails/generators"
require "rails/generators/active_record"

module ActiveStorageReplicated
  module Generators
    # Copies the migration for the gem's two tables and an initializer with every setting. Table names come
    # from the app's settings, so set custom names in the initializer before running the generator.
    class InstallGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      class_option :database, type: :string, aliases: "--db",
        desc: "Database for the gem's tables, as named in database.yml. Defaults to config.database, or the primary one."

      def create_migration_file
        migration_template(
          "create_active_storage_replicated_tables.rb.tt",
          File.join(db_migrate_path, "create_active_storage_replicated_tables.rb"),
        )
      end

      def create_initializer
        template("initializer.rb.tt", "config/initializers/active_storage_replicated.rb")
      end

      private

      def migration_version
        "[#{ActiveRecord::VERSION::STRING.to_f}]"
      end

      # Puts the migration where the chosen database keeps its migrations.
      def db_migrate_path
        database = options[:database].presence || ActiveStorageReplicated.config.database&.to_s
        if database.nil?
          return super
        end

        config = ActiveRecord::Base.configurations.configs_for(env_name: Rails.env, name: database)
        config&.migrations_paths ? Array(config.migrations_paths).first : super
      end

      def database_name
        options[:database].presence || ActiveStorageReplicated.config.database&.to_s
      end

      def tasks_table
        ActiveStorageReplicated.config.table_name
      end

      def audits_table
        ActiveStorageReplicated.config.audits_table_name
      end
    end
  end
end
