class CreateActiveStorageReplicatedTables < ActiveRecord::Migration[7.2]
  def change
    create_table :active_storage_replicated_tasks do |t|
      t.string :service_name, null: false
      t.string :target, null: false
      t.integer :operation, null: false
      t.string :key, null: false
      t.string :source
      t.integer :state, null: false, default: 0
      t.integer :attempts, null: false, default: 0
      t.text :last_error
      t.datetime :run_after, null: false
      t.datetime :claimed_until
      t.string :claimed_by
      t.timestamps

      t.index [ :service_name, :target, :operation, :id ], where: "state = 0", name: "index_active_storage_replicated_tasks_queue"
      t.index [ :service_name, :target, :operation, :run_after ], where: "state = 0", name: "index_active_storage_replicated_tasks_due"
      t.index [ :service_name, :key, :target ], where: "state = 0", name: "index_active_storage_replicated_tasks_pending"
      t.index [ :service_name, :key ], where: "state = 0", opclass: { key: :text_pattern_ops }, name: "index_active_storage_replicated_tasks_prefix"
      t.index [ :service_name, :key ], where: "state = 1 AND operation IN (1, 2)", name: "index_active_storage_replicated_tasks_tombstones"
      t.index :updated_at, where: "state <> 0", name: "index_active_storage_replicated_tasks_finished"
      t.index :service_name, where: "operation = 2 AND key NOT LIKE '%/'", name: "index_active_storage_replicated_tasks_prefix_deletes"
    end

    create_table :active_storage_replicated_audits do |t|
      t.string :service_name, null: false
      t.date :run_on, null: false
      t.datetime :started_at, null: false
      t.datetime :heartbeat_at, null: false
      t.datetime :finished_at
      t.integer :attempts, null: false, default: 1
      t.jsonb :summary
      t.text :error
      t.timestamps

      t.index [ :service_name, :run_on ], unique: true
    end
  end
end
