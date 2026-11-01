require "bundler/setup"

APP_RAKEFILE = File.expand_path("test/dummy/Rakefile", __dir__)
load "rails/tasks/engine.rake"

require "bundler/gem_tasks"

desc "Run the test suite on Rails 7.2"
task "test:rails_7_2" do
  Bundler.with_unbundled_env do
    sh({ "BUNDLE_GEMFILE" => File.expand_path("gemfiles/rails_7_2.gemfile", __dir__) }, "bin/rails test")
  end
end

desc "Rebuild the test app's database and dump its schema with Rails 7.2, the oldest supported version"
task "test:dump_schema" do
  Bundler.with_unbundled_env do
    env = { "BUNDLE_GEMFILE" => File.expand_path("gemfiles/rails_7_2.gemfile", __dir__) }
    Dir.chdir("test/dummy") do
      sh(env, "bin/rails db:drop db:create db:migrate db:schema:dump")
    end
  end
end
