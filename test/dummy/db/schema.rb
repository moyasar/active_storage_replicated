# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[7.2].define(version: 2026_11_01_004852) do
  # These are extensions that must be enabled in order to support this database
  enable_extension "plpgsql"

  create_table "active_storage_attachments", force: :cascade do |t|
    t.string "name", null: false
    t.string "record_type", null: false
    t.bigint "record_id", null: false
    t.bigint "blob_id", null: false
    t.datetime "created_at", null: false
    t.index ["blob_id"], name: "index_active_storage_attachments_on_blob_id"
    t.index ["record_type", "record_id", "name", "blob_id"], name: "index_active_storage_attachments_uniqueness", unique: true
  end

  create_table "active_storage_blobs", force: :cascade do |t|
    t.string "key", null: false
    t.string "filename", null: false
    t.string "content_type"
    t.text "metadata"
    t.string "service_name", null: false
    t.bigint "byte_size", null: false
    t.string "checksum"
    t.datetime "created_at", null: false
    t.index ["key"], name: "index_active_storage_blobs_on_key", unique: true
  end

  create_table "active_storage_replicated_audits", force: :cascade do |t|
    t.string "service_name", null: false
    t.date "run_on", null: false
    t.datetime "started_at", null: false
    t.datetime "heartbeat_at", null: false
    t.datetime "finished_at"
    t.integer "attempts", default: 1, null: false
    t.jsonb "summary"
    t.text "error"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["service_name", "run_on"], name: "idx_on_service_name_run_on_66ab8eee24", unique: true
  end

  create_table "active_storage_replicated_tasks", force: :cascade do |t|
    t.string "service_name", null: false
    t.string "target", null: false
    t.integer "operation", null: false
    t.string "key", null: false
    t.string "source"
    t.integer "state", default: 0, null: false
    t.integer "attempts", default: 0, null: false
    t.text "last_error"
    t.datetime "run_after", null: false
    t.datetime "claimed_until"
    t.string "claimed_by"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["service_name", "key", "target"], name: "index_active_storage_replicated_tasks_pending", where: "(state = 0)"
    t.index ["service_name", "key"], name: "index_active_storage_replicated_tasks_prefix", opclass: { key: :text_pattern_ops }, where: "(state = 0)"
    t.index ["service_name", "key"], name: "index_active_storage_replicated_tasks_tombstones", where: "((state = 1) AND (operation = ANY (ARRAY[1, 2])))"
    t.index ["service_name", "target", "operation", "id"], name: "index_active_storage_replicated_tasks_queue", where: "(state = 0)"
    t.index ["service_name", "target", "operation", "run_after"], name: "index_active_storage_replicated_tasks_due", where: "(state = 0)"
    t.index ["service_name"], name: "index_active_storage_replicated_tasks_prefix_deletes", where: "((operation = 2) AND ((key)::text !~~ '%/'::text))"
    t.index ["updated_at"], name: "index_active_storage_replicated_tasks_finished", where: "(state <> 0)"
  end

  create_table "active_storage_variant_records", force: :cascade do |t|
    t.bigint "blob_id", null: false
    t.string "variation_digest", null: false
    t.index ["blob_id", "variation_digest"], name: "index_active_storage_variant_records_uniqueness", unique: true
  end

  create_table "documents", force: :cascade do |t|
    t.string "name"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

  add_foreign_key "active_storage_attachments", "active_storage_blobs", column: "blob_id"
  add_foreign_key "active_storage_variant_records", "active_storage_blobs", column: "blob_id"
end
