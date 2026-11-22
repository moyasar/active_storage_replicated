require "test_helper"

class RedirectTest < ActionDispatch::IntegrationTest
  include ReplicationHelpers

  setup do
    @service = build_replicated_service
    @primary, @replica = @service.targets
    registered_services[:replicated_test] = @service
    @service.targets.each { |target| registered_services[target.name.to_sym] = target }
  end

  teardown do
    registered_services.delete(:replicated_test)
    @service.targets.each { |target| registered_services.delete(target.name.to_sym) }
    ActiveStorageReplicated.config.redirect_url_options = nil
  end

  test "a link redirects to the first endpoint while it is healthy" do
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))

    get link_path("invoices/1.pdf")

    assert_response :redirect
    assert_equal "primary", redirected_target
    assert_equal "no-store", response.headers["Cache-Control"]
  end

  test "a link sent before an endpoint went down redirects to one that answers" do
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))
    path = link_path("invoices/1.pdf")
    @primary.down = true

    get path

    assert_response :redirect
    assert_equal "replica", redirected_target
  end

  test "an endpoint still waiting for its copy is skipped" do
    store(@replica, "invoices/1.pdf", "pdf")
    enqueue("primary", :copy, "invoices/1.pdf")

    get link_path("invoices/1.pdf")

    assert_equal "replica", redirected_target
  end

  test "the link keeps the file name, disposition and content type" do
    @service.upload("exports/1", StringIO.new("a,b"))

    get link_path("exports/1", filename: "payments.csv", disposition: :attachment, content_type: "text/csv")
    follow_redirect!

    assert_response :success
    assert_equal "text/csv", response.media_type
    assert_match(/attachment; filename="payments.csv"/, response.headers["Content-Disposition"])
  end

  test "an expired link is not found" do
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))
    path = link_path("invoices/1.pdf", expires_in: 1.hour)

    travel 2.hours do
      get path
    end

    assert_response :not_found
  end

  test "a tampered link is not found" do
    get "/rails/active_storage/replicated/forged"

    assert_response :not_found
  end

  test "a deleted file is not found" do
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))
    path = link_path("invoices/1.pdf")
    @service.delete("invoices/1.pdf")

    get path

    assert_response :not_found
  end

  test "links use redirect_url_options for their host" do
    ActiveStorageReplicated.config.redirect_url_options = { host: "files.example.com", protocol: "https" }

    url = @service.redirect_url("invoices/1.pdf", expires_in: 1.day)

    assert_match %r{\Ahttps://files\.example\.com/rails/active_storage/replicated/}, url
  end

  test "a link needs a lifetime" do
    [ nil, 0, -5 ].each do |lifetime|
      assert_raises(ArgumentError) { @service.redirect_url("invoices/1.pdf", expires_in: lifetime, host: "www.example.com") }
    end
  end

  test "a service that is not in storage.yml cannot make links" do
    unregistered = ActiveStorage::Service::ReplicatedService.new(targets: @service.targets).tap { |service| service.name = :unregistered }

    error = assert_raises(ArgumentError) { unregistered.redirect_url("invoices/1.pdf", expires_in: 1.day, host: "www.example.com") }
    assert_match(/storage.yml/, error.message)
  end

  test "a link without a host fails when it is made, not when it is opened" do
    assert_raises(ArgumentError) { @service.redirect_url("invoices/1.pdf", expires_in: 1.day) }
  end

  test "an error while signing the link does not count against the endpoint" do
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))
    @primary.define_singleton_method(:url) { |*, **| raise ArgumentError, "bad options" }

    3.times do
      assert_raises(ArgumentError) { @service.checked_url("invoices/1.pdf", expires_in: 5.minutes) }
    end

    assert_empty ActiveStorageReplicated.health.down_names
  end

  test "every endpoint down is an error, not a missing file" do
    @service.upload("invoices/1.pdf", StringIO.new("pdf"))
    path = link_path("invoices/1.pdf")
    @service.targets.each { |target| target.down = true }

    assert_raises(Errno::ECONNREFUSED) { get path }
  end

  test "a service that is not replicated gets a plain link" do
    service = ActiveStorage::Blob.services.fetch(:test)
    service.upload("plain/1.txt", StringIO.new("x"))

    get URI(ActiveStorageReplicated.redirect_url("plain/1.txt", service: service, expires_in: 1.day, host: "www.example.com")).path

    assert_response :redirect
    assert_match %r{/rails/active_storage/disk/}, response.location
  ensure
    service.delete("plain/1.txt")
  end

  private

  def registered_services
    ActiveStorage::Blob.services.send(:services)
  end

  def link_path(key, expires_in: 7.days, **options)
    URI(@service.redirect_url(key, expires_in: expires_in, host: "www.example.com", **options)).path
  end

  def redirected_target
    encoded = URI(response.location).path.split("/")[4]
    ActiveStorage.verifier.verified(CGI.unescape(encoded), purpose: :blob_key)["service_name"].to_s
  end
end
