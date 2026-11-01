namespace :active_storage_replicated do
  desc "Run pending replication tasks once"
  task run: :environment do
    puts(ActiveStorageReplicated::Runner.run_once.inspect)
  end

  desc "Compare the endpoints of every replicated service and queue tasks for any difference"
  task audit: :environment do
    puts(ActiveStorageReplicated::Audit.run.inspect)
  end

  desc "Drop pending tasks for an endpoint removed from storage.yml: rake active_storage_replicated:forget_target[service,target]"
  task :forget_target, [ :service, :target ] => :environment do |_task, args|
    if args[:service].blank? || args[:target].blank?
      abort("Usage: rake active_storage_replicated:forget_target[service,target]")
    end

    count = ActiveStorageReplicated::Task.pending
      .where(service_name: args[:service], target: args[:target])
      .update_all(state: ActiveStorageReplicated::Task.states[:skipped], last_error: "endpoint removed", claimed_by: nil, claimed_until: nil)
    puts("#{count} pending tasks for #{args[:service]}/#{args[:target]} dropped")
  end
end
