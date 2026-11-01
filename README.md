# ActiveStorageReplicated

An Active Storage service that keeps every file on two or more S3-compatible endpoints.
If one endpoint goes down, the app keeps working. When the endpoint comes back, it catches up by itself.

| Operation | What happens |
| --- | --- |
| Upload | Goes to healthy endpoints until `sync_copies` of them have it, all of them by default, in parallel. The rest get a copy task. Fails if no endpoint took the file, or if the file does not match its checksum. |
| Delete | Records a task per endpoint, then deletes. Never raises because of an endpoint or the database. |
| Read | Uses an endpoint known to have the current file. Endpoints still waiting for a copy are skipped. A plain "not found" costs one request. |
| Health | Tracked in memory in each process. Three endpoint errors in a row mark an endpoint down for 30 seconds. In Puma workers the background thread then probes it, so user requests never wait on a dead endpoint. Processes without it, such as Sidekiq, let one call through after each cool-off. |
| Catch-up | A thread in each Puma worker runs pending tasks every 15 seconds. Each task is leased to one worker, and no database transaction is held while files move. |
| Audit | Once a day, compares the endpoints and queues tasks for any difference. |

Requires Rails 7.2 or 8.x, Ruby 3.3 or newer, and Postgres.

---

## Setup

### Step 1. Add the gem

```ruby
# Gemfile
gem "active_storage_replicated", github: "moyasar/active_storage_replicated", ref: "<commit or tag>"
```

```shell
bundle install
```

Pin a commit or release tag, so every deploy gets the same code.

### Step 2. Run the installer

```shell
bin/rails g active_storage_replicated:install
```

It creates two files:

| File | Content |
| --- | --- |
| `db/migrate/..._create_active_storage_replicated_tables.rb` | Two tables. `active_storage_replicated_tasks` holds pending copies and deletes. `active_storage_replicated_audits` holds one row per daily audit. |
| `config/initializers/active_storage_replicated.rb` | Every setting, commented out with its default. |

To use other table names, set `table_name` and `audits_table_name` in the initializer first, then run the installer.
To keep the tables in another database, pass `--database name`: the migration goes to that database's migrations
folder and the initializer gets `config.database = :name`.

### Step 3. Migrate

```shell
bin/rails db:migrate
```

### Step 4. Put the endpoint keys in Rails credentials

The gem reads no environment variables. Keep secrets in the app's encrypted credentials:

```shell
bin/rails credentials:edit --environment production
```

```yaml
storage:
  primary:
    access_key_id: AKIA...
    secret_access_key: ...
  replica:
    access_key_id: ...
    secret_access_key: ...
```

Each key needs these permissions on its bucket: `s3:ListBucket`, `s3:GetObject`, `s3:PutObject` and
`s3:DeleteObject`.

> Without `s3:ListBucket`, a missing file answers 403 instead of 404. The gem reads 403 as refused credentials, so
> ordinary "not found" reads and the health probe would mark the endpoint down.

### Step 5. Describe the endpoints in `config/storage.yml`

Each endpoint is an ordinary Active Storage service. The replicated service lists them under `targets`.
Settings that are not secret are written in the file as they are.

```yaml
platform_bucket:
  service: Replicated
  targets: [ platform_primary, platform_replica ]

platform_primary:
  service: S3
  access_key_id: <%= Rails.application.credentials.dig(:storage, :primary, :access_key_id) %>
  secret_access_key: <%= Rails.application.credentials.dig(:storage, :primary, :secret_access_key) %>
  region: me-east-1
  bucket: platform-primary
  endpoint: https://s3.primary.example.com
  force_path_style: false
  http_open_timeout: 3
  http_read_timeout: 15
  retry_mode: standard
  max_attempts: 1

platform_replica:
  service: S3
  access_key_id: <%= Rails.application.credentials.dig(:storage, :replica, :access_key_id) %>
  secret_access_key: <%= Rails.application.credentials.dig(:storage, :replica, :secret_access_key) %>
  region: us-east-1
  bucket: platform-replica
  endpoint: https://s3.replica.example.com
  force_path_style: false
  http_open_timeout: 3
  http_read_timeout: 15
  retry_mode: standard
  max_attempts: 1
```

For different buckets per environment, put each environment's values in its own credentials, or use a storage file
per environment: Rails loads `config/storage/production.yml` when it exists.

**The replicated service**

| Key | Required | Meaning |
| --- | --- | --- |
| `service` | Yes | Always `Replicated`. |
| `targets` | Yes | Names of two or more services in this file. The order matters: the first endpoint is preferred for reads and links, and it is the reference when the audit finds two different versions of a file. |
| `public` | No | `true` for a bucket whose files are publicly readable. Set it on each target too. Default `false`. |

**Each target**

| Key | Required | Meaning |
| --- | --- | --- |
| `service` | Yes | `S3` for any S3-compatible endpoint. `Disk` with a `root` folder also works, for development. |
| `bucket` | Yes | Bucket name. Each target usually has its own bucket. |
| `endpoint` | For non-AWS | Endpoint address. Leave it out for AWS. |
| `region` | Yes | Region name. Local test servers accept `us-east-1`. |
| `access_key_id`, `secret_access_key` | Yes | Keys for this endpoint, read from Rails credentials. |
| `force_path_style` | No | `true` puts the bucket in the path instead of the host name. Most local test servers need `true`. |
| `http_open_timeout` | Recommended | Seconds to wait for a connection. Use `3`. The AWS default is 15. |
| `http_read_timeout` | Recommended | Seconds to wait for each piece of a response. Use `15`. The AWS default is 60. |
| `retry_mode`, `max_attempts` | Recommended | Use `standard` and `1`. The gem retries by itself, so retries inside the AWS client only add waiting. |
| `request_checksum_calculation`, `response_checksum_validation` | If needed | Recent AWS SDK versions send CRC32 checksums by default. If an S3-compatible endpoint rejects uploads with a checksum error, set both to `when_required`. |
| `public` | No | `true` makes uploads publicly readable. Match the replicated service. |
| `upload` | No | Extra options sent with every upload, for example `{ server_side_encryption: AES256 }`. |

> Keep the short timeouts and `max_attempts: 1`. Without them, a hung endpoint can hold each call for a minute or more before it is marked down.

### Step 6. Make it the default service, if you want

```ruby
# config/environments/production.rb
config.active_storage.service = :platform_bucket
```

To keep files that already exist, give the replicated service the same name as your current service, and list the
current bucket first in `targets`. Existing blobs store the service name, so they work without a data migration.
The first audit then copies the files the new endpoint lacks.

> For a bucket that already holds many files, copy it to the new endpoint first with a bulk tool such as
> `rclone sync`. The audit settles differences; it does not move a bucket. It queues one task per file and
> every copy flows through a Puma worker, 100 tasks at a time.

### Step 7. Start the background thread

```ruby
# config/puma.rb
workers 4
plugin :active_storage_replicated
```

- It starts its threads in every Puma worker, or in the single process when Puma runs without workers.
- It works with or without `preload_app!`, and with workers set in the config file, on the command line, or through
  `WEB_CONCURRENCY`.
- It works with Puma 6, 7 and 8.
- The catch-up, its lease renewal, the daily audit and the outage retry each use a database connection while they
  work. Make the pool four larger than Puma's threads per worker.
- For another server, call `ActiveStorageReplicated::Scheduler.start` once in each worker process after boot.

Processes that do not run Puma, such as Sidekiq, use the service normally. Their missed work is picked up by the
Puma workers.

### Step 8. Adjust the settings, if needed

`config/initializers/active_storage_replicated.rb`:

```ruby
ActiveStorageReplicated.configure do |config|
  config.sync_copies = :all
  config.interval = 15
  config.audit_at = "03:30"
end
```

Every setting is checked when it is set, so a wrong value fails at boot.

| Setting | Default | Meaning |
| --- | --- | --- |
| `sync_copies` | `:all` | How many endpoints an upload waits for before it returns. See "How an upload spreads". |
| `parallel_writes` | `true` | With `sync_copies = :all`, write to all endpoints at the same time. Used when the file can be reopened (a file on disk or a `StringIO`); other streams are written one endpoint after another. |
| `database` | `nil` | Database for the gem's tables, as named in `database.yml`. `nil` uses the app's main database and its transactions. Setting it opens a separate connection pool, so plan for more connections, and task writes no longer join the app's transactions. |
| `table_name` | `"active_storage_replicated_tasks"` | Name of the to-do table. |
| `audits_table_name` | `"active_storage_replicated_audits"` | Name of the audit table. |
| `interval` | `15` | Seconds between background runs in each worker. |
| `failure_threshold` | `3` | Endpoint errors in a row before an endpoint is skipped. |
| `cool_off` | `30` | Seconds before a down endpoint is probed again. |
| `probe_key` | `"active_storage_replicated/health-check"` | Key the probe asks about. It does not need to exist, given `s3:ListBucket` (Step 4). |
| `lease` | `600` | Seconds a claimed task stays reserved for the worker that claimed it. Must be longer than `time_budget`. |
| `batch_size` | `100` | Tasks a worker claims at a time. |
| `time_budget` | `50` | Seconds one background run may work. It is checked before each task; unstarted tasks are given back. |
| `max_backoff` | `900` | Longest wait in seconds between retries of a failing task. Retries start at 30 seconds and double. |
| `alert_after_attempts` | `10` | A task that has failed this many times is reported once through `Rails.error`. |
| `retention` | `2592000` | Seconds to keep finished tasks, 30 days. Finished deletes prove to the audit that a file was removed on purpose. |
| `direct_upload_grace` | `60` | Seconds added to a direct upload link's lifetime before its copies are attempted. |
| `audit_at` | `"03:30"` | Daily audit time, in the app's time zone. `nil` turns the automatic audit off. |
| `audit_min_age` | `3600` | Seconds. The audit ignores files younger than this. |
| `audit_prefixes` | `nil` | Key prefixes the audit compares, or `nil` for the whole bucket. Prefixes must not overlap. |
| `audit_sort_limit` | `200000` | Keys the audit may sort in memory when an endpoint does not list keys in order. Above it, that audit fails and is reported. Sorting takes roughly 0.5 MB of memory per 1,000 keys per endpoint. |
| `read_fallback_on_not_found` | `false` | When an endpoint answers "not found", ask the next one too. Off by default, so a real 404 costs one request. Turn it on if files can go missing from an endpoint outside the gem, for example after restoring a bucket. |
| `audit_compare_checksums` | `true` | Compare ETags when two copies have the same size. Turn it off for S3-compatible stores whose ETags are not MD5 checksums. |
| `logger` | `nil` | Logger. `nil` uses `Rails.logger`. |

### How an upload spreads

| `sync_copies` | Before `upload` returns | After it returns |
| --- | --- | --- |
| `:all` | Every healthy endpoint has the file. | Endpoints that were down get it from the catch-up. |
| `1` | One endpoint has it: the first in `targets` order that accepts it. | The others get it from the catch-up, usually within one `interval`. |
| `2`, `3`, ... | That many endpoints have it, tried in order. | The rest get it from the catch-up. |

- **A number above the number of endpoints** acts like `:all`.
- **If fewer endpoints are healthy than you asked for,** the upload still succeeds with what it got.
- **`1` is the fastest.** The cost: for a few seconds the file exists on one endpoint only, and each background copy
  downloads the file again from that endpoint.
- **`stream_upload` always writes one endpoint first,** because a stream cannot be replayed.

---

## Use it in a model

Nothing changes in model code.

```ruby
class Contract < ApplicationRecord
  has_one_attached :appendix                            # uses the default service
  has_one_attached :scan, service: :platform_bucket     # or name it
end
```

```ruby
contract.appendix.attach(io: File.open("appendix.pdf"), filename: "appendix.pdf")   # written per sync_copies
contract.appendix.download                                                         # from an endpoint with the current file
contract.appendix.url                                                              # link to such an endpoint
contract.appendix.purge                                                            # deleted everywhere, or queued
```

`has_many_attached`, variants and direct uploads from the browser work too. A direct upload goes to one endpoint,
and copies to the others are queued for when its link expires.

---

## Use it without a model

Get the service, then call it directly. Keys are file paths inside the bucket.

```ruby
storage = ActiveStorage::Blob.service                            # the default service
storage = ActiveStorage::Blob.services.fetch(:platform_bucket)   # or by name
```

| Call | What it does |
| --- | --- |
| `storage.upload(key, io)` | Writes `io` to healthy endpoints until `sync_copies` have it. |
| `storage.stream_upload(key, content_type:) { \|io\| ... }` | Writes a file you generate piece by piece. See below. |
| `storage.download(key)` | Returns the whole file as a string. |
| `storage.download(key) { \|chunk\| ... }` | Streams the file in chunks. |
| `storage.download_chunk(key, range)` | Returns part of the file, for example `0..1023`. |
| `storage.exist?(key)` | `true` if an endpoint with the current file has it. Raises if it cannot tell because endpoints failed. |
| `storage.url(key, expires_in:, filename:, disposition:, content_type:)` | A signed link that expires, or a public link on a public service. |
| `storage.delete(key)` | Deletes from every endpoint, or queues the delete. |
| `storage.delete_many(keys)` | `delete` for each key. |
| `storage.delete_prefixed(prefix)` | Deletes every key that starts with `prefix`. |

```ruby
storage.upload("invoices/1.pdf", StringIO.new(pdf))
storage.download("invoices/1.pdf")
storage.download("statements/1.csv") { |chunk| file.write(chunk) }
storage.url("invoices/1.pdf", expires_in: 1.hour, filename: ActiveStorage::Filename.new("invoice.pdf"),
  disposition: :inline, content_type: "application/pdf")
storage.delete("invoices/1.pdf")
```

Reads and links run three small indexed queries on the to-do table, to skip endpoints that are still waiting for a
copy or a delete. If they fail, the read carries on without them.

### Stream a large generated file

Use `stream_upload` for exports you write piece by piece. On S3 it uses a multipart upload, so the file never has
to fit in memory.

```ruby
storage.stream_upload("exports/1.csv", content_type: "text/csv") do |io|
  csv = CSV.new(io)
  rows.each { |row| csv << row }
end
```

- The stream goes to the first healthy endpoint. If that endpoint fails before your block writes anything, the next
  endpoint is tried.
- If it fails after your block started, the call raises, because a stream cannot be replayed.
- If your block raises, you get your own exception back, and the endpoint is not blamed.
- The catch-up copies the file to the other endpoints a few seconds later.

### Reach one endpoint directly

To skip replication, for example to inspect one bucket, use the target's own service:

```ruby
primary = ActiveStorage::Blob.services.fetch(:platform_bucket).target_named(:platform_primary)
primary.exist?("invoices/1.pdf")
primary.bucket    # the Aws::S3::Bucket, for anything Active Storage does not cover
```

Writes and deletes made this way are not replicated. The daily audit copies such files later. A file deleted this
way comes back from the other endpoint, because the audit deletes only when a recorded delete is newer than the file.
The audit also allows one minute for clock differences between the app and the endpoints before it trusts a delete.

---

## Operate it

```shell
bin/rails active_storage_replicated:run     # run pending tasks once
bin/rails active_storage_replicated:audit   # compare the endpoints now
```

```ruby
ActiveStorageReplicated.status
# => { "platform_bucket" => { pending: { "platform_replica/copy" => 12 }, oldest_pending_at: ..., stuck: 0,
#                             down_in_this_process: [], waiting_in_memory_in_this_process: 0,
#                             last_audit: { run_on: ..., summary: ..., error: nil } } }
```

### Monitoring

Background failures are logged with a backtrace and sent to `Rails.error`, so your error tracker sees them.
These events are published through `ActiveSupport::Notifications`:

| Event | When |
| --- | --- |
| `endpoint_down.active_storage_replicated` | An endpoint is marked down in a process. |
| `endpoint_up.active_storage_replicated` | It answers again. |
| `operation_failed.active_storage_replicated` | One call to one endpoint failed. |
| `task_stuck.active_storage_replicated` | A task reached `alert_after_attempts` failures. |
| `run.active_storage_replicated` | A background run finished, with its summary. |
| `audit.active_storage_replicated` | An audit finished, with its summary. |

```ruby
ActiveSupport::Notifications.subscribe("endpoint_down.active_storage_replicated") do |*, payload|
  Rails.logger.warn("storage endpoint down: #{payload[:target]}")
end
```

Alert on a growing `pending` count or a rising `oldest_pending_at`: either means an endpoint is still down. Both drain
by themselves when it is back.

### What happens when

| Situation | Behaviour |
| --- | --- |
| An endpoint refuses connections | Fails fast. After three errors it is skipped, and its work is queued. |
| An endpoint hangs | The first calls wait up to the timeouts. After three it is skipped. The background thread probes it every `cool_off`. |
| The endpoint returns | The probe marks it up. The backlog drains in the background. |
| The database is down | Uploads, reads and deletes still reach storage. The bookkeeping they could not write is kept in memory and written within seconds of the database coming back, by whichever process holds it. It is lost only if the process stops first; that is reported, and a delete lost that way may come back with the audit. |
| A request and the catch-up touch the same file at once | The catch-up holds a lease on its task. The request never writes that endpoint; it queues its change behind the task. If no other endpoint accepts the file, the upload fails rather than racing. |
| A copy runs longer than the lease | The worker renews the lease while it works and checks it right before writing. If it may have written after losing the lease, it queues a settle for that endpoint (see below). |
| A write is interrupted halfway (a request timeout) | The work it replaced is put back, and a settle is queued for every endpoint five minutes later, after any writes still in flight have finished. |
| Copies and the audit | Every copy carries the time its content was first written, so the audit never takes a late copy of an old file for a newer one, and never lets it outrank a delete. |
| A worker dies in the middle of a task | The task's lease expires and another worker takes it. |
| A worker dies during the audit | Another process takes the audit over once its heartbeat is ten minutes old. |
| A deploy adds or renames an endpoint | Workers that do not know it yet leave its tasks for the workers that do. |
| An endpoint is removed from `storage.yml` | Its pending tasks stay. Drop them with `bin/rails "active_storage_replicated:forget_target[service,target]"`. |
| An app sends GET requests to a read replica | The gem's own reads and writes always use the writing role. |
| A folder delete runs late, after new files were written into the folder | Only files written more than a minute before the delete was asked for are removed; anything closer is kept, because file times can be whole seconds and clocks differ. |
| Every other endpoint is still catching up on a file | The copy settles instead of trusting an endpoint that may hold an older version. |
| An endpoint refuses the credentials (401 or 403) | It is marked down like an unreachable one, and its work waits. |
| A failed upload had replaced pending work | The replaced copies and deletes are put back. |

**Settling a file.** When task history cannot say which endpoint is right (a correction after a lost lease, an
interrupted write, endpoints waiting on each other), the runner looks at what every endpoint holds now. Every
endpoint must answer, or it tries again later. Endpoints with a pending delete count as not holding the file. Then:

- If a delete was asked for clearly after the newest content, the endpoint's copy is deleted.
- If another endpoint holds newer content, it is copied here.
- If this endpoint holds the newest content, the endpoints without the file get a copy from it.
- A file is never deleted without a recorded or pending delete as proof.

### Replacing or restoring an endpoint

- **A new, empty endpoint:** list it last in `targets` until the first audit after adding it has finished and its
  copies have drained. Listed first, it would answer "not found" for every file it does not have yet.
- **An endpoint restored from a backup:** it holds older versions and files deleted since the backup. The audit
  replaces older versions with newer ones, and deletes files whose delete records are still kept (`retention`,
  30 days by default). Files deleted longer ago than that come back. Raise `retention` before restoring if needed.
  Until that audit has finished, list it last in `targets` (or turn on `read_fallback_on_not_found`). Listed
  first, it answers "not found" for files written after the backup and serves older versions of changed files.

### Apps that share buckets

When two apps write to the same buckets, each has its own to-do table, so neither sees the other's pending copies
or deletes. A file one app wrote while an endpoint was down is unknown to the other app, which may read the
endpoint that does not have it yet and answer "not found". Give both apps one table:

- Point `database` in one app at a connection to the other app's database that may write the two gem tables.
- Use the same service name and the same target names for the same bucket in both `storage.yml` files.

If the tables must stay separate, turn on `read_fallback_on_not_found` in both apps, run the audit in one app only,
and accept that a delete made by the other app can be undone by that audit.

---

## Try it locally

Run two S3-compatible endpoints in development, bound to `127.0.0.1`:

```shell
docker run -d --name s3-a -p 127.0.0.1:19000:9000 rclone/rclone:1.71.1 serve s3 /data --addr :9000 --auth-key devkey,devsecret123
docker run -d --name s3-b -p 127.0.0.1:19090:9090 adobe/s3mock:latest
```

Point the targets at `http://127.0.0.1:19000` and `http://127.0.0.1:19090`. Use region `us-east-1`,
`force_path_style: true`, and keys `devkey` and `devsecret123`.

| To test | Do |
| --- | --- |
| An endpoint that refuses connections | `docker stop s3-a`, later `docker start s3-a`. rclone keeps its files. |
| An endpoint that hangs | `docker pause s3-b`, later `docker unpause s3-b`. |

> s3mock starts empty after `docker stop` and `docker start`. Use `pause` with it, or the restart looks like a wiped endpoint.
> rclone's S3 server does not list keys in sorted order, so the audit sorts its listings in memory.

---

## Limits

- A stream goes to one endpoint first, then is copied.
- A direct upload that finishes later than its link's lifetime is copied when the next attempt finds it; copies wait
  for a late upload for up to a day.
- Links saved in your database point at one endpoint and do not follow failover.
- A file on an endpoint with SSE-KMS or customer keys cannot be checked by checksum; the audit compares its size only.
- Postgres only: the gem uses row locks with `SKIP LOCKED`, partial indexes, `jsonb` and the database clock for leases.
- The gem's bookkeeping has no timeout of its own. Behind PgBouncer, set `query_wait_timeout` and the app's
  `connect_timeout` and `checkout_timeout` low, so a database outage fails fast instead of holding storage calls.
- `ActiveStorageReplicated.status` counts the whole backlog; call it from a dashboard, not from a frequent health check.
- Two requests writing the same key at the same moment are not ordered against each other; the endpoints may hold
  different versions until the audit settles on the newest. Active Storage blob keys are unique, so this only concerns
  keys you choose yourself.
- A file deleted within a minute of being written, while a copy of it is in flight and the database is unreachable,
  can come back with the audit.
- A worker stopped during `stream_upload`, or during a copy over 100 MB, leaves an incomplete multipart upload
  behind. Add a lifecycle rule to each bucket that aborts incomplete multipart uploads after a day.
- Copies and uploads that cannot be rewound pass through a temporary file, so the host needs free temp space for
  the largest file.
- After an upload interrupted by a request timeout, the key may read as "not found" for up to five minutes if only
  the later endpoint received the file; the settle that follows copies it everywhere.

---

## Develop the gem

The test app's database settings are in `test/dummy/config/database.yml`: Postgres on `127.0.0.1:55432`, user and
password `postgres`.

```shell
docker run -d --name asr-pg -p 127.0.0.1:55432:5432 -e POSTGRES_PASSWORD=postgres postgres:17

bin/rails db:test:prepare
bin/rails test                         # Rails 8.1
bundle exec rake test:rails_7_2        # Rails 7.2, first run: bundle install --gemfile gemfiles/rails_7_2.gemfile
```

The test app's schema is dumped with Rails 7.2, so both versions can load it. After changing its migrations, run
`bundle exec rake test:dump_schema`.

The real S3 tests run only when `test/s3_endpoints.yml` exists. Start the two endpoints from "Try it locally", then:

```shell
cp test/s3_endpoints.example.yml test/s3_endpoints.yml
bin/rails test test/integration/s3_endpoints_test.rb
```

CI runs everything, including the real S3 tests against two s3mock containers, on Rails 8.1 and 7.2.

## License

MIT.
