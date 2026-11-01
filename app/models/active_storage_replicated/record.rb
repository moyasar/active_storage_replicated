module ActiveStorageReplicated
  # Base class for the gem's tables, so an app can point them at a database of its choice.
  class Record < ActiveRecord::Base
    self.abstract_class = true
  end
end

ActiveSupport.run_load_hooks(:active_storage_replicated_record, ActiveStorageReplicated::Record)
