module ActiveStorageReplicated
  class Engine < ::Rails::Engine
    initializer "active_storage_replicated.database" do
      ActiveSupport.on_load(:active_storage_replicated_record) do
        database = ActiveStorageReplicated.config.database
        if database
          connects_to(database: { writing: database, reading: database })
        end
      end
    end

    config.after_initialize do
      ActiveStorageReplicated.config.validate!
    end
  end
end
