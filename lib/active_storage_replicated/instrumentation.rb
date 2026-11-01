module ActiveStorageReplicated
  # Events published through ActiveSupport::Notifications under the "active_storage_replicated" namespace,
  # plus error reporting through Rails.error for failures that happen in the background.
  module Instrumentation
    module_function

    def instrument(event, payload = {}, &block)
      ActiveSupport::Notifications.instrument("#{event}.active_storage_replicated", payload, &block)
    end

    def report(error, context = {})
      ActiveStorageReplicated.logger.error(
        "[ActiveStorageReplicated] #{context[:where]} failed: #{error.class}: #{error.message}\n" \
        "#{Array(error.backtrace).first(10).join("\n")}",
      )

      if Rails.respond_to?(:error)
        Rails.error.report(error, handled: true, source: "active_storage_replicated", context: context)
      end
    rescue StandardError
      nil
    end
  end
end
