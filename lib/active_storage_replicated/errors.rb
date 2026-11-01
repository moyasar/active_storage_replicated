module ActiveStorageReplicated
  # Decides whether an error means the endpoint itself is unhealthy, as opposed to a problem with one file or
  # one request. Only endpoint errors count toward marking an endpoint down.
  module Errors
    # Besides 5xx: a timeout, throttling, and refused credentials (expired or revoked keys make the whole
    # endpoint unusable).
    CLIENT_STATUSES_THAT_MEAN_DOWN = [ 401, 403, 408, 429 ].freeze

    module_function

    # A failure on this machine, such as a full disk for a temporary file, that says nothing about the endpoint.
    class LocalFailure < StandardError; end

    def endpoint_error?(error)
      if error.is_a?(ActiveStorage::FileNotFoundError) || error.is_a?(ActiveStorage::IntegrityError) ||
          error.is_a?(LocalFailure) || error.is_a?(IOError) || client_side?(error)
        return false
      end

      status = http_status(error)
      if status
        return status >= 500 || CLIENT_STATUSES_THAT_MEAN_DOWN.include?(status)
      end

      true
    end

    # Raised before a request is sent, such as aws-sdk rejecting a parameter, so it is about the call, not the
    # endpoint.
    def client_side?(error)
      error.is_a?(ArgumentError) || error.is_a?(TypeError) || error.is_a?(NameError)
    end

    def http_status(error)
      if defined?(Aws::Errors::ServiceError) && error.is_a?(Aws::Errors::ServiceError)
        error.context&.http_response&.status_code
      end
    end

    def database_error?(error)
      error.is_a?(ActiveRecord::ActiveRecordError)
    end
  end
end
