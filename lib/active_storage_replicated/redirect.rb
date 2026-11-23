module ActiveStorageReplicated
  module Redirect
    PURPOSE = "active_storage_replicated_redirect"

    class << self
      def url(key, expires_in:, service: ActiveStorage::Blob.service, filename: nil, disposition: :inline, content_type: nil, **url_options)
        unless expires_in.respond_to?(:positive?) && expires_in.positive?
          raise ArgumentError, "expires_in must be a positive duration, got #{expires_in.inspect}"
        end

        unless registered?(service)
          raise ArgumentError, "#{service.name.inspect} is not a service in storage.yml, so its links could not be opened"
        end

        token = verifier.generate(
          {
            "service" => service.name.to_s,
            "key" => key,
            "filename" => (filename || File.basename(key)).to_s,
            "disposition" => disposition.to_s,
            "content_type" => content_type
          },
          expires_in: expires_in,
          purpose: PURPOSE,
        )

        Rails.application.routes.url_helpers.active_storage_replicated_redirect_url(token, **link_url_options(url_options))
      end

      def target_url(token)
        link = verifier.verified(token.to_s, purpose: PURPOSE)
        if link.nil?
          return nil
        end

        service = ActiveStorage::Blob.services.fetch(link["service"]) { return nil }
        options = {
          expires_in: ActiveStorage.service_urls_expire_in,
          filename: ActiveStorage::Filename.new(link["filename"]),
          disposition: link["disposition"].to_sym,
          content_type: link["content_type"]
        }

        if service.respond_to?(:checked_url)
          service.checked_url(link["key"], **options)
        else
          service.url(link["key"], **options)
        end
      end

      def verifier
        @verifier ||= ActiveSupport::MessageVerifier.new(
          Rails.application.key_generator.generate_key(PURPOSE),
          digest: "SHA256",
          url_safe: true,
          serializer: JSON,
        )
      end

      private

      def registered?(service)
        ActiveStorage::Blob.services.fetch(service.name.to_s) { nil }.equal?(service)
      end

      def link_url_options(url_options)
        base = ActiveStorageReplicated.config.redirect_url_options || ActiveStorage::Current.url_options || {}
        base.to_h.symbolize_keys.merge(url_options)
      end
    end
  end
end
