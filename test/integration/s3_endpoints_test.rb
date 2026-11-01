require "test_helper"
require "active_storage/service/s3_service"

# Runs against two real S3-compatible endpoints. Skipped unless test/s3_endpoints.yml exists;
# copy test/s3_endpoints.example.yml to create it.
class S3EndpointsTest < ActiveSupport::TestCase
  include ReplicationHelpers

  SETTINGS_FILE = File.expand_path("../s3_endpoints.yml", __dir__)

  setup do
    unless File.exist?(SETTINGS_FILE)
      skip "test/s3_endpoints.yml is missing"
    end

    @settings = YAML.load_file(SETTINGS_FILE)
    %w[primary replica].each do |side|
      unless reachable?(@settings.fetch(side))
        skip "#{@settings.fetch(side)} is not answering; start the endpoints from the README"
      end
    end

    @prefix = "asr-test/#{SecureRandom.hex(4)}"
    @primary = s3_target("s3_primary", @settings.fetch("primary"), @settings.fetch("primary_bucket", "asr-primary"))
    @replica = s3_target("s3_replica", @settings.fetch("replica"), @settings.fetch("replica_bucket", "asr-replica"))
    @service = replicated(@primary, @replica)
  end

  test "upload, read and delete reach both endpoints" do
    key = "#{@prefix}/a.txt"

    @service.upload(key, StringIO.new("both"))

    assert @primary.exist?(key)
    assert @replica.exist?(key)
    assert_equal "both", @service.download(key)
    assert_equal 0, ActiveStorageReplicated::Task.count

    @service.delete(key)

    assert_not @primary.exist?(key)
    assert_not @replica.exist?(key)
  end

  test "an unreachable endpoint is skipped and caught up from the other" do
    dead = s3_target("s3_dead", "http://127.0.0.1:1", "asr-dead")
    service = replicated(dead, @replica)
    key = "#{@prefix}/failover.txt"

    service.upload(key, StringIO.new("kept"))

    assert @replica.exist?(key)
    assert_equal "kept", service.download(key)
    assert ActiveStorageReplicated::Task.pending.exists?(target: "s3_dead", key: key)
  end

  test "stream_upload writes a multipart file to one endpoint and the runner copies it" do
    key = "#{@prefix}/export.csv"
    line = "#{"x" * 1023}\n"

    @service.stream_upload(key, content_type: "text/csv") do |io|
      6_000.times { io.write(line) }
    end

    assert_equal 6_000 * 1024, @primary.bucket.object(key).content_length
    assert_not @replica.exist?(key)

    ActiveStorageReplicated::Runner.run_once([ @service ])

    assert_equal 6_000 * 1024, @replica.bucket.object(key).content_length
  end

  test "the audit lists both buckets and queues a copy for a missing file" do
    key = "#{@prefix}/only-primary.txt"
    @primary.upload(key, StringIO.new("one"))
    ActiveStorageReplicated.config.audit_prefixes = [ "#{@prefix}/" ]

    summary = ActiveStorageReplicated::Audit.new(@service, now: 2.hours.from_now).run

    assert_operator summary[:copies], :>=, 1
    assert ActiveStorageReplicated::Task.pending.exists?(target: "s3_replica", key: key)
  ensure
    ActiveStorageReplicated.config.audit_prefixes = nil
  end

  test "a signed link points at an endpoint that has the file" do
    key = "#{@prefix}/link.txt"
    @replica.upload(key, StringIO.new("link"))
    ActiveStorageReplicated::Task.enqueue!(service_name: :s3_test, target: "s3_primary", operation: :copy, key: key)

    url = @service.url(key, expires_in: 5.minutes, filename: ActiveStorage::Filename.new("link.txt"),
      disposition: :inline, content_type: "text/plain")

    assert_includes url, URI(@settings.fetch("replica")).port.to_s
  end

  test "a catch-up copy keeps the content type, disposition and metadata" do
    key = "#{@prefix}/typed.pdf"
    @primary.upload(key, StringIO.new("%PDF"), content_type: "application/pdf", filename: ActiveStorage::Filename.new("typed.pdf"),
      disposition: :inline, custom_metadata: { "origin" => "test" })
    ActiveStorageReplicated::Task.enqueue!(service_name: :s3_test, target: "s3_replica", operation: :copy, key: key)

    ActiveStorageReplicated::Runner.run_once([ @service ])

    copied = @replica.bucket.object(key)
    copied.load
    assert_equal "application/pdf", copied.content_type
    assert_match(/inline/, copied.content_disposition.to_s)
    assert_equal "test", copied.metadata["origin"]
  end

  test "stream_upload moves to the next endpoint when the first fails before any byte" do
    dead = s3_target("s3_dead", "http://127.0.0.1:1", "asr-dead")
    service = replicated(dead, @replica)
    key = "#{@prefix}/fallback.csv"

    service.stream_upload(key, content_type: "text/csv") { |io| io.write("a,b") }

    assert_equal "a,b", @replica.download(key)
    assert ActiveStorageReplicated::Task.pending.exists?(target: "s3_dead", key: key)
  end

  test "stream_upload gives the caller back its own exception" do
    custom = Class.new(StandardError)

    assert_raises(custom) do
      @service.stream_upload("#{@prefix}/broken.csv") do |io|
        io.write("a" * 10)
        raise custom
      end
    end
    assert_not ActiveStorageReplicated.health.down?("s3_primary")
  end

  test "the audit finds files with the same size but different content, and copies the newer one" do
    key = "#{@prefix}/same-size.txt"
    @primary.upload(key, StringIO.new("aaaa"))
    sleep(1.1) # S3 file times have one-second precision; a tie goes to the first endpoint
    @replica.upload(key, StringIO.new("bbbb"))
    ActiveStorageReplicated.config.audit_prefixes = [ "#{@prefix}/" ]

    ActiveStorageReplicated::Audit.new(@service, now: 2.hours.from_now).run
    task = ActiveStorageReplicated::Task.pending.find_by!(key: key)

    assert_equal [ "s3_primary", "audit:s3_replica" ], [ task.target, task.source ]
  ensure
    ActiveStorageReplicated.config.audit_prefixes = nil
  end

  test "parallel writes from a file reach both endpoints" do
    key = "#{@prefix}/parallel.bin"
    file = Tempfile.new("parallel")
    file.write("x" * 1024)
    file.flush

    @service.upload(key, file)

    assert_equal 1024, @primary.bucket.object(key).content_length
    assert_equal 1024, @replica.bucket.object(key).content_length
  ensure
    file&.close!
  end

  private

  def s3_target(name, endpoint, bucket)
    target = ActiveStorage::Service::S3Service.new(
      bucket: bucket,
      endpoint: endpoint,
      region: @settings.fetch("region", "us-east-1"),
      force_path_style: @settings.fetch("force_path_style", true),
      access_key_id: @settings.fetch("access_key_id"),
      secret_access_key: @settings.fetch("secret_access_key"),
      http_open_timeout: 2,
      http_read_timeout: 15,
      retry_mode: "standard",
      max_attempts: 1,
    )
    target.name = name

    if @settings.fetch("create_buckets", true) && !endpoint.end_with?(":1")
      begin
        target.client.client.create_bucket(bucket: bucket)
      rescue Aws::S3::Errors::BucketAlreadyOwnedByYou, Aws::S3::Errors::BucketAlreadyExists
        nil
      end
    end

    target
  end

  # TCPSocket.new is Ruby's C implementation and answers the same on Linux and macOS: refused at once, a silent
  # host after connect_timeout (IO::TimeoutError), an unknown name within resolv_timeout. Socket.tcp with a
  # timeout misreports refused connects on macOS.
  def reachable?(endpoint)
    uri = URI.parse(endpoint)
    TCPSocket.new(uri.hostname, uri.port, resolv_timeout: 1, connect_timeout: 1).close
    true
  rescue SystemCallError, SocketError, IO::TimeoutError
    false
  end

  def replicated(*targets)
    ActiveStorage::Service::ReplicatedService.new(targets: targets).tap { |service| service.name = :s3_test }
  end
end
