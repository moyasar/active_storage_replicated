require "test_helper"

# Goes through Active Storage the way an app does: storage.yml, has_one_attached, download, purge.
class AttachmentTest < ActiveSupport::TestCase
  include ReplicationHelpers

  setup do
    @service = ActiveStorage::Blob.services.fetch(:replicated)
    @primary, @replica = @service.targets
  end

  teardown do
    @service.targets.each { |target| FileUtils.rm_rf(target.root) }
  end

  test "storage.yml builds a replicated service from its targets" do
    assert_instance_of ActiveStorage::Service::ReplicatedService, @service
    assert_equal [ "replicated_primary", "replicated_replica" ], @service.targets.map(&:name).map(&:to_s)
    assert_equal [ :replicated ], ActiveStorageReplicated.services.map(&:name)
  end

  test "attach, download and purge go through both endpoints" do
    document = Document.create!(name: "contract")
    document.file.attach(io: StringIO.new("signed"), filename: "contract.pdf", content_type: "application/pdf")
    key = document.file.blob.key

    assert_equal "replicated", document.file.blob.service_name
    assert_equal "signed", read_back(@primary, key)
    assert_equal "signed", read_back(@replica, key)
    assert_equal "signed", document.file.download

    document.file.purge

    assert_not stored?(@primary, key)
    assert_not stored?(@replica, key)
  end
end
