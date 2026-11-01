require_relative "lib/active_storage_replicated/version"

Gem::Specification.new do |spec|
  spec.name        = "active_storage_replicated"
  spec.version     = ActiveStorageReplicated::VERSION
  spec.authors     = [ "Omar Alshboul" ]
  spec.email       = [ "omar.abdula@gmail.com" ]
  spec.homepage    = "https://github.com/moyasar/active_storage_replicated"
  spec.summary     = "Active Storage service that replicates files across several endpoints with automatic failover."
  spec.description = "Writes and deletes go to every healthy endpoint, reads come from the first one that works, " \
                     "and a Postgres to-do table catches up whatever an endpoint missed while it was down."
  spec.license     = "MIT"

  spec.required_ruby_version = ">= 3.3"

  # Blocks an accidental push to rubygems.org. Change it to the registry you publish to.
  spec.metadata["allowed_push_host"] = "https://rubygems.pkg.github.com/moyasar"
  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    Dir["{app,lib}/**/*", "MIT-LICENSE", "README.md"]
  end

  spec.add_dependency "activerecord", ">= 7.2", "< 9"
  spec.add_dependency "activestorage", ">= 7.2", "< 9"
  spec.add_dependency "railties", ">= 7.2", "< 9"
  spec.add_dependency "concurrent-ruby", "~> 1.1"
  spec.add_dependency "pg", ">= 1.1"
end
