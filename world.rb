require 'set'
require 'logger'
require 'csv'
require 'observer'

require_relative 'events/input_log_event'
require_relative 'events/synthetic_event'
require_relative 'state_updater'
require_relative 'modules/constants'
require_relative 'lru'
require_relative 'modules/errors'
require_relative 'yarn_scheduler'

# Every resource (cluster, host, colloc or anything else added in the future)
# is getting registered with the world
# at the time of the creation on its constructor.
# World is responsible to register the new resource with all the related
# entities on its "register" method.
# Similarly a job will register with the cluster and the cluster is
# responsible to register the job with the host.
# Jobs are not directly accessed through the world as they cannot exist
# without a cluster.
# methods with _signal postfix determine changes to internal state required
# by an external event that the object itself is not aware.
# For example finished jobs are determined by world and then signals are
# send to clusters, hosts and jobs
class World
  include Observable

  attr_accessor :clusters,
  :hosts,
  :collocs,
  # <(cluster_name, host_name, job_id), Task<Status, Count>>
  :tasks_map,
  #:hosts_cluster_map, #Mappings of hosts to clusters
  :last_seen_jobs_on_host,
  :last_seen_jobs_on_cluster,
  :last_seen_augmented_jobs_on_host,
  :last_seen_augmented_jobs_on_cluster,
  :finished_augm_jobs_signals_map,
  :running_tasks_count,
  :waiting_tasks_count,
  :last_processed_timestamp,
  :ticks,
  :last_tick_change,
  :granularity,
  :inventory_hosts_count,
  :events_received,
  :events_received_this_timestamp,
  :observers
  def initialize(conf)
    configure(conf)
    @clusters = Hash.new(0)
    @collocs = Hash.new(0)
    @hosts = Hash.new(0)
    @tasks_map = Hash.new(0)
    @ticks = 0
    @last_tick_change = 0
    @granularity = Constants::Numeric::GRANULARITY
    # For example ACQUIRED status for a particular job running on a host
    @last_seen_task_statuses_of_job_on_host = Lru.new(-1, @granularity)

    # Keep unlimited job keys that are not older than granularity seconds
    # than the current timestamp
    @last_seen_jobs_on_host = Lru.new(-1, @granularity)
    @last_seen_jobs_on_cluster = Lru.new(-1, @granularity)
    @last_seen_augmented_jobs_on_host = Lru.new(-1, @granularity)

    # Keep separate track of augmented_jobs. When a job is finished on the augmented hosts
    # we send signals to explicitely finish everything else still running
    @last_seen_augmented_jobs_on_cluster = Lru.new(-1, @granularity)
    # Keep track of the finished jobs signals so we can ignore updates for these jobs and also
    # avoid re-sending finished signals for augmented jobs that can lead to inconsistencies
    # finished_augm_jobs_signals_map <cluster_job_key, true> , true if signal already send
    @finished_augm_jobs_signals_map = Hash.new
    @observers = []
    @events_received = 0
    @events_received_this_timestamp = 0
    @processed_clusters = {}
    @processed_hosts = {}
    @processed_jobs = {}

    # job_metrics and hosts_metrics are imported from a file to estimate the max number of
    # tasks that can fit to a provisioned host and the avg time needed to execute the task
    @jobs_metrics = {} # <cluster_name, job_id, avg_task_size, avg_run_time>
    @hosts_metrics = {} # <host_name, memory_size>

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::INFO
  end

  def print_summary
    puts '------------------ Summary -------------------'
    puts "Clusters count: #{@clusters.size}"
    puts "Hosts count: #{@hosts.size}"
    puts "Collocs: #{get_collocs_names}"
    puts "Clusters: #{get_clusters_names}"

    @collocs.each { |_name, colloc| colloc.print_summary }
    @clusters.each { |_name, cluster| cluster.print_summary }
  end

  def print_all
    puts '------------- Collocs ------------------------'
    @collocs.each { |_name, colloc| colloc.print_all }
    puts "\n"

    puts '------------- Clusters -----------------------'
    @clusters.each do |_name, cluster|
      # cluster.print_all
      # cluster.print_summary
      puts "\n"
      cluster.print(true, false, false, false)
    end
  end

  def create_stats(timestamp)
    @collocs.each_value { |c| c.create_stats(timestamp) }
  end

  def print_stats
    @collocs.each do |name, c|
      puts "#{name}:"
      c.print_stats
    end
  end

  # Method called externally by the world_state object whenever the timestamp increases
  def timestamp_changed_signal(timestamp)
    timestamp_changed(timestamp)
    if timestamp > @creation_time
      if (@ticks % @granularity == 0) # Calculate every 1 hour

        now = Time.now
        @logger.info "Current time: #{timestamp} - #{Time.at(timestamp)}"
        @logger.info "Hours processed: #{@ticks / @granularity} - events: #{@events_received}"

        processing_time = 0
        processing_time = (now - @last_tick_change).to_f unless @last_tick_change == 0
        @logger.info "Last hour processed in (sec): #{processing_time}"
        unless processing_time == 0
          @logger.info "Events/sec (this hour): #{@events_received_this_timestamp/processing_time}"
        end
        print_stats
        @last_tick_change = now
        @events_received_this_timestamp = 0
      end
      @ticks += 1

      # Send when all events of the timestamp are processed
      send_finished_signals(timestamp)
      send_timestamp_changed_signals(timestamp)
      # Get values only when the timestamp changes to make sure all events
      # of the same timestamp are processed
      # print_offer_and_demand
      create_stats(timestamp)
      @logger.debug "*** Timestamp #{timestamp-60} processed!\n"
    end
  end

  def send_timestamp_changed_signals(timestamp)
    #@processed_jobs.timestamp_processed_signal(timestamp)
    #@processed_hosts.timestamp_processed_signal(timestamp)
    # Order is important! Hosts should update their status first so the can send the
    # host_status_changed signal to the cluster before the cluster runs its own
    # timestamp_changed method that includes sanity checks
    send_timestamp_changed_to_processed_resource(timestamp, @processed_jobs)
    send_timestamp_changed_to_processed_resource(timestamp, @processed_hosts)
    send_timestamp_changed_to_processed_resource(timestamp, @processed_clusters)

    @processed_clusters = {}
    @processed_hosts = {}
    @processed_jobs = {}
  end

  def update(state_updater, event)
    @logger.level = Logger::INFO
    @events_received += 1
    @events_received_this_timestamp += 1
    if event.is_a?(ControlEvent)
      if (event.message == Constants::ControlMessages::SIM_IS_OVER)
        @logger.info "Received '#{event.message}' event!"
        return
      elsif (event.message == Constants::ControlMessages::GAP)
        @logger.debug "Received '#{event.message}' event with timestamp: #{event.timestamp}!"
        return
      end
    elsif (event.is_a?(SchedulingEvent))
      process_scheduling_event(event)
      return #Do nothing at the moment
    end

    # OPTIMIZE: Instead of treating finished jobs differently I could create
    # finished events and run update methods on all affected entities

    # Sending finished signals should preceed the scheduling decisions
    # so it cannot be an observer to the world_state like the scheduler is.

    colloc_name = event.host_name.split('.')[1]
    # Create objects if they don't already exist in the world
    colloc, cluster, host, job = get_affected_resources(event.timestamp,
                                                        colloc_name,
                                                        event.cluster_name,
                                                        event.host_name,
                                                        event.job_id,
                                                        event.task_status)

    @logger.debug 'Updating state with event:'
    event.print if @logger.debug?

    # Update tasks_map and get diff from previous state
    cluster_job_key = ClusterJobKey.new(event.cluster_name, event.job_id)

    host_job_key = HostJobKey.new(event.cluster_name, event.host_name, event.job_id)
    # Have to track the statuses that appear on each minute, so they can be reseted to zero when
    # they don't appear for a minute.
    # There still will be 1min delay to update the status of a task that is going for example from
    # acquired to running state on the same minute. We don't have TASK IDs on the input -
    # so we cannot be sure about task status transitions.
    host_job_status_key = HostJobStatusKey.new(event.cluster_name,
    event.host_name,
    event.job_id,
    event.task_status)

    # Ignore input events coming for a finished augmented job
    # TODO: Create host_job_keys for all hosts of the cluster and delete. Without that
    # the host_job_key has to appear before being able to delete

    if (@finished_augm_jobs_signals_map.has_key?(cluster_job_key) &&
    @finished_augm_jobs_signals_map[cluster_job_key])
      @logger.debug "Ignoring..."
      cluster_job_key.print if @logger.debug?
      # delete_from_lru_cache(host_job_status_key, host_job_key, cluster_job_key)
      return
    end

    # Add resources that are processed on this timestamp
    # in order to send to all of them the timestamp_processed_signal
    # that triggers them to update their status according to their logic
    @processed_clusters[cluster.name] = cluster
    @processed_hosts[host.name] = host unless event.task_status == Constants::TaskStatus::REQUESTED
    @processed_jobs[job.id] = job

    # Job on this host is already augmented by running tasks on a provisioned host
    if(cluster.yarn_scheduler.augments_job_on_host(host_job_key) &&
    event.task_status == Constants::TaskStatus::REQUESTED)

      event.task_count = cluster.yarn_scheduler.calc_req_event_count(host_job_key, event.task_count)
      @logger.debug "Remaining waiting tasks (after delegations): #{event.task_count}"
      if (event.task_count == 0)
        @logger.debug "Ignoring event..."
        event.print if @logger.debug?
        return
      end
    end

    # For synthetic events the task_count is -1 until we give it the actual value at this point
    # See comments on yarn_scheduler.generate_run_event to see why is this so
    if (event.is_a?(SyntheticEvent))
      event.task_count = cluster.yarn_scheduler.calc_run_event_count(event)
      @logger.debug "Synthetic event:"
      event.print if @logger.debug?
    end

    # Deal with waiting tasks when additional provisioned resources exist
    if (event.task_status == Constants::TaskStatus::REQUESTED && \
    cluster.provisioned_hosts_count > 0)
      synthetic_events, wait_count = cluster.yarn_scheduler.delegate_event(event,
      host_job_key)

      if (synthetic_events.size > 0)
        @logger.debug "Delegated tasks for #{job.id}:"
        synthetic_events.each do |e|
          e.print if @logger.debug?
        end
        job.augmented = true # Sufficient capacity on host to augment part of these job's tasks
      end

      event.task_count = wait_count # Update the event with the remaining waiting tasks
      cluster.yarn_scheduler.enqueue_events(state_updater, synthetic_events)
      # If there are no events left in REQUESTED status no need to process the event
      if (wait_count == 0)
        @logger.debug "All tasks for #{job.id} delegated!"
        return
      else
        @logger.debug "Resources not sufficient to delegate all tasks! Remainder: #{wait_count}"
      end
    end

    virtual = (job.augmented && !host.nil? && host.provisioned)? true : false
    update_lru_cache(event.timestamp, host_job_status_key, host_job_key, cluster_job_key, virtual)

    # Host changed! Make changes before updates happen
    if host_changed_cluster?(event.host_name, event.cluster_name)
      move_host_to_cluster(event.host_name, event.cluster_name)
    end

    # The following updates should happen only after the "update_finished_jobs" is called,
    # as this will trigger external changes to object and then the object has
    # to update their internal state to be consistent

    # Gets the difference of tasks for the particular host and job based on this event
    # and compared to the previous count.
    # This works only for the statuses that still have at least one task running.
    # The rest are treated with signals.
    task_diff = set_tasks_count(host_job_key, event.task_status, event.task_count)
    update_rest_of_world(event, cluster, host, job, task_diff)
  end

  def process_scheduling_event(scheduling_event)
    @logger.debug "Processing scheduling event..."
    scheduling_event.print if @logger.debug?
    if scheduling_event.is_a?(EvictionEvent)
      process_eviction_event(scheduling_event)
    elsif scheduling_event.is_a?(ProvisionEvent)
      process_provision_event(scheduling_event)
    else
      fail UnknownSchedulingEventTypeError, 
        "Scheduling type #{scheduling_event.class} not supported!"
    end
  end

  def register_cluster(cluster, colloc_name)
    @clusters[cluster.name] = cluster
    register_cluster_with_colloc(cluster, colloc_name)
  end

  def register_colloc(colloc)
    @collocs[colloc.name] = colloc
  end

  def register_host(host, colloc_name)
    # OPTIMIZE: Host didn't have access to cluster and colloc object at the time this method
    # was implemented. Consider making registration directly from host to cluster and colloc
    @hosts[host.name] = host
    # OPTIMIZE: Consider doing the registrations on host, cluster or colloc updates
    register_host_with_colloc(host, colloc_name)
  end

  def get_full_clusters
    @collocs.each_values do |col|
      full_clusters.merge(col.full_clusters)
    end
    full_clusters
  end

  private

  ### Methods related to assigning waiting tasks to provisioned hosts through YarnScheduler ###

  ######

  def configure(conf)
    @inventory_hosts_count = conf['inventory_hosts_count']
  end

  def register_cluster_with_colloc(cluster, colloc_name)
    @collocs[colloc_name].register_cluster(cluster)
  end

  def register_host_with_colloc(host, colloc_name)
    @collocs[colloc_name].register_host(host)
  end

  def register_host_with_cluster(host, cluster_name)
    @clusters[cluster_name].register_host(host)
  end

  def deregister_host_from_cluster(host_name, cluster_name)
    @clusters[cluster_name].deregister_host(host_name)
  end

  def get_affected_resources(timestamp, colloc_name, cluster_name, host_name, job_id, task_status)
    colloc = (collocs.key?(colloc_name)) ? collocs[colloc_name] : Colloc.new(self, colloc_name)
    cluster = (clusters.key?(cluster_name)) ? clusters[cluster_name] : Cluster.new(self, cluster_name, colloc)
    if hosts.key?(host_name)
      host = hosts[host_name]

      if(host.status == Constants::HostStatus::INVENTORY \
      || host.status == Constants::HostStatus::OFFLINE)      
        @logger.debug "#{timestamp} Re-registering #{host.name} with cluster #{cluster.name}"
        # Will be re-registered with a cluster here
        unless (task_status == Constants::TaskStatus::REQUESTED)
          host.configure(timestamp, cluster, task_status)
        end
      end
    else
      unless (task_status == Constants::TaskStatus::REQUESTED)
        host = Host.new(self, colloc, host_name)
        host.configure(timestamp, cluster, task_status) # Will be registered with a cluster here
      end
    end
    job = (cluster.has_job(job_id)) ? cluster.get_job(job_id) : Job.new(cluster, job_id)

    [colloc, cluster, host, job]
  end

  def send_timestamp_changed_to_processed_resource(timestamp, resource)
    # Informs resources (cluster, hosts, jobs) that the timestamp changed
    return if resource.nil?
    resource.each_value do |r|
      r.timestamp_processed_signal(timestamp)
    end
  end

  def send_finished_signals(timestamp)
    # Dealing with Jobs and Task statuses that no longer appear on the updates. For example jobs
    # that are finished on a host or they have completed their execution on the cluster or a task
    # status that doesn't have any task to represent it on the current timestamp
    # (for example 0 tasks on acquired status)
    #
    # NOTE: With the current algorithm jobs are found when clock ticks 2 min after the last time
    # the job was seen running. For example if the last updated is on timestamp 1420099200
    # the job will be marked as finished on the first event with timestamp 1420099320
    # , since at this point all event updates with timestamps 1420099260 are done!

    not_existing_task_statuses_of_job_on_host = @last_seen_task_statuses_of_job_on_host.expire(timestamp)
    # Process all jobs that haven't appear for more than granularity seconds on a host
    finished_jobs_on_host = @last_seen_jobs_on_host.expire(timestamp)
    finished_augmented_jobs_on_host = @last_seen_augmented_jobs_on_host.expire(timestamp)

    finished_jobs_on_cluster = @last_seen_jobs_on_cluster.expire(timestamp)
    finished_augmented_jobs_on_cluster = @last_seen_augmented_jobs_on_cluster.expire(timestamp)

    # Send signals for the jobs that no longer run on provisioned hosts
    unless finished_augmented_jobs_on_host.nil?
      if(finished_jobs_on_host.nil?)
        finished_jobs_on_host = finished_augmented_jobs_on_host
      else
        finished_jobs_on_host.merge(finished_augmented_jobs_on_host)
      end
    end

    unless finished_augmented_jobs_on_cluster.nil?
      if(finished_jobs_on_cluster.nil?)
        finished_jobs_on_cluster = finished_augmented_jobs_on_cluster
      else
        finished_jobs_on_cluster.merge(finished_augmented_jobs_on_cluster)
      end
    end

    # Keep to the global set of finished augmented jobs

    unless(finished_augmented_jobs_on_cluster.nil?)
      finished_augmented_jobs_on_cluster.each do |cluster_job_key|
        @finished_augm_jobs_signals_map[cluster_job_key] = false # Signal not yet sent
      end
    end

    reset_task_statuses_count(timestamp, not_existing_task_statuses_of_job_on_host)
    update_finished_jobs(timestamp, finished_jobs_on_cluster, finished_jobs_on_host)
  end

  # Required to reset the counters for all the statuses that stop appearing apart from the RUNNING
  # statuses that will be treated separately with the job_finished_signals
  def reset_task_statuses_count(timestamp, not_existing_task_statuses_of_job_on_host)
    return if not_existing_task_statuses_of_job_on_host.nil? || 
              not_existing_task_statuses_of_job_on_host.empty?
    not_existing_task_statuses_of_job_on_host.each do |t|

      host_job_key = HostJobKey.new(t.cluster_name, t.host_name, t.job_id)
      cluster_job_key = ClusterJobKey.new(t.cluster_name, t.job_id)

      @logger.level = Logger::INFO
      
    # Necessary to avoid reducing counters that are already reseted to zero because jobs where
    # forced to finish
    if (@finished_augm_jobs_signals_map.has_key?(cluster_job_key) &&
        @finished_augm_jobs_signals_map[cluster_job_key] == true)
        @logger.debug "Skipping..."
        cluster_job_key.print if @logger.debug?
        # sleep 1
        next # Don't send anything for jobs already done
    end
    
      # Diff is not directly set to 0 because we want to reduce from the task
      # counters of the cluster and host, that might have way more tasks on this
      # state from other jobs

      # puts "#{diff} - #{host_job_key.job_id} - #{host_job_key.host_name}"

      # RUNNING status will be treated with finished signals
      @logger.debug "#{timestamp}: Resetting #{t.task_status} for:"
      host_job_key.print if @logger.debug?

      if t.task_status == Constants::TaskStatus::RUNNING
        @logger.debug "Skipped!"
      else
        diff = 0 - get_tasks_count(host_job_key, t.task_status)
        set_tasks_count(host_job_key, t.task_status, 0)
        @clusters[t.cluster_name].task_status_update_signal(t.host_name, t.job_id, t.task_status, diff)
      end
    end
  end

  def set_tasks_count(key, task_status, task_count)
    fail RangeError, "Tasks cannot be less than zero! (Found: #{task_count})" unless task_count >= 0
    @tasks_map[key] = TaskStatusCounters.new('world', key) unless @tasks_map.key?(key)
    diff = task_count - @tasks_map[key].get_count_of_status(task_status)
    @tasks_map[key].set_count_of_status(task_status, task_count)
    diff
  end

  def get_tasks_count(key, task_status)
    get_tasks_map_with_key(key).get_count_of_status(task_status)
  end

  def update_finished_jobs(timestamp, finished_jobs_on_cluster, finished_jobs_on_host)
    update_finished_jobs_on_cluster(timestamp, finished_jobs_on_cluster)
    update_finished_jobs_on_host(timestamp, finished_jobs_on_host)
  end

  def update_finished_jobs_on_cluster(timestamp, finished_jobs_on_cluster)
    # finished_jobs_on_cluster is a set of unique ClusterJobKeys <cluster_name, job_id>
    return if finished_jobs_on_cluster.nil? || finished_jobs_on_cluster.size == 0
    finished_jobs_on_cluster.each do |cluster_job_key|
      if (@finished_augm_jobs_signals_map.has_key?(cluster_job_key) &&
      @finished_augm_jobs_signals_map[cluster_job_key] == true)
        next
      end
      job_id = cluster_job_key.job_id
      cluster_name = cluster_job_key.cluster_name

      @logger.debug "#{timestamp}:cluster job_finished_signal for #{job_id}"

      @clusters[cluster_name].job_finished_signal(job_id)

      # Send finished signals to other hosts running jobs for this cluster as the end of an
      # augmented job on provisioned hosts dictates the finish of the job overall
      # remaining tasks on the hosts are task already served on provisioned hosts
      # XXX: These hosts should only be hosts that where contained on the transformed REQUESTED
      # events. Other hosts its ok to keep on running tasks as their task run_time can be greater
      # than the average time that we ran on provisioned hosts or additional tasks is possible that
      # where added later
      if(@clusters[cluster_name].jobs[job_id].augmented)
        @logger.debug "Delegated job #{job_id} done!"
        @finished_augm_jobs_signals_map[cluster_job_key] = true

        forced_finished_task_statuses_of_job_on_host = Set.new
        other_hosts_running_job = Set.new

        @clusters[cluster_name].hosts.each_value do |host|
          if(host.runs_job?(job_id))
            @logger.debug "Sending force-finished signals to: #{cluster_name} - #{host.name} - #{job_id}"
            forced_finished_task_statuses_of_job_on_host.add(HostJobStatusKey.new(cluster_name, host.name, job_id, Constants::TaskStatus::RUNNING))
            other_hosts_running_job.add(HostJobKey.new(cluster_name, host.name, job_id))
          end
        end

        reset_task_statuses_count(timestamp, forced_finished_task_statuses_of_job_on_host)
        # The provisioned hosts will receive the finished_job_on_host signal twice but its ok
        # because they will ignore duplicates
        update_finished_jobs_on_host(timestamp, other_hosts_running_job)

        # TODO: delete all necessary LRUs - after this point there should be no updates for this job!
        # delete_from_lru_cache(host_job_status_key, host_job_key, cluster_job_key)
        # @last_seen_jobs_on_cluster.delete(cluster_job_key)
      end

      # TODO: Runs something similar to below to stop tracking anything for jobs that got augmented
      # and are now finished
      # delete_from_lru_cache(host_job_status_key, host_job_key, cluster_job_key)

    end
  end

  # This method is triggered from job_hosts keys that no longer appear on the input indicating that
  # the job of this key stopped running on the host that this key indicates
  # Signal sent initially to the cluster so the cluster can update its internal
  # task_status_counters by reducing the tasks related to this job_host and then the cluster will
  # also send the relevant signals to the related job and host so they can also update their counters
  # and state
  def update_finished_jobs_on_host(timestamp, finished_jobs_on_host)
    # finished_jobs_on_host is a set of unique HostJobKeys <cluster_name, host_name, job_id>
    return if finished_jobs_on_host.nil? || finished_jobs_on_host.size == 0

    finished_jobs_on_host.each do |host_job_key|
      job_on_host_task_status_counters = get_tasks_map_with_key(host_job_key)

      # Without the check signals might be send to hosts that have jobs on "REQUESTED" state
      # - With the check on inconsistencies appear (ex: Busy hosts = 4 and running job status
      # counters=0) but they will be fixed when all object are updated at the end of the timestamp

      @logger.debug "#{timestamp}: job_done_on_host signal
                                #{host_job_key.job_id}
                                on #{host_job_key.host_name}
                                of #{host_job_key.cluster_name}"
      host_job_key.print if @logger.debug?
      @logger.debug "counters:"
      job_on_host_task_status_counters.print if @logger.debug?

      if (@clusters[host_job_key.cluster_name].jobs[host_job_key.job_id].augmented)
        @logger.debug "augmented job"
      end

      @clusters[host_job_key.cluster_name].job_done_on_host_signal( @last_processed_timestamp,
      host_job_key.job_id,
      host_job_key.host_name,
      job_on_host_task_status_counters)
      job_on_host_task_status_counters.reset_counters # Reset world counters for this key
    end
  end

  def delete_from_lru_cache(host_job_status_key, host_job_key, cluster_job_key)
    # TODO: Obsolete - Consider removing!
    @last_seen_task_statuses_of_job_on_host.delete(host_job_status_key)
    @last_seen_jobs_on_host.delete(host_job_key)
    @last_seen_jobs_on_cluster.delete(cluster_job_key)
  end

  def update_lru_cache(timestamp, host_job_status_key, host_job_key, cluster_job_key, virtual = false)
    @last_seen_task_statuses_of_job_on_host[host_job_status_key] = timestamp
    @last_seen_jobs_on_host[host_job_key] = timestamp
    @last_seen_jobs_on_cluster[cluster_job_key] = timestamp
    if virtual
      @last_seen_augmented_jobs_on_host[host_job_key] = timestamp
      @last_seen_augmented_jobs_on_cluster[cluster_job_key] = timestamp
    end
  end

  def host_changed_cluster?(host_name, event_cluster_name)
    return false unless @hosts.has_key?(host_name)
    (@hosts[host_name].cluster_name != event_cluster_name) ? true : false
  end

  def move_host_to_cluster(host_name, new_cluster_name)
    old_cluster_name = @hosts[host_name].cluster_name
    deregister_host_from_cluster(host_name, old_cluster_name)
    register_host_with_cluster(@hosts[host_name], new_cluster_name)
    @logger.info "Cluster for host #{host_name} changed from: #{old_cluster_name} to: #{new_cluster_name}"
  end

  def update_rest_of_world(event, cluster, host, job, task_diff)
    # Correct update depends on the order of updates. Job update should run first.

    job.update(event, task_diff, cluster)
    #Hosts are not necessarily created the time they appear because the status can be REQUESTED
    host.update(event, task_diff, cluster) unless host.nil?
    cluster.update(event, task_diff)
    #No need to update colloc --> will be updated with signals every time host status is updated
  rescue => e
    @logger.error e.message
    e.backtrace.each { |line| @logger.error line }
    raise
  end

  def print_offer_and_demand
    # *Used only to create a graph - not part of the code flaw
    logger = Logger.new('offer_and_demand-7days.csv')
    logger.level = Logger::INFO
    if @collocs.key?('sc1')
      offer = @collocs['sc1'].free_hosts_count
      demand = @collocs['sc1'].full_clusters_count
      all_clusters = @collocs['sc1'].clusters_count
      jobs_waiting = waiting_jobs_of_colloc('sc1')
      tasks_waiting = waiting_tasks_of_colloc('sc1')

      logger.<< "#{offer},#{demand},#{all_clusters},#{jobs_waiting},#{tasks_waiting}\n"
      # sanity_check_on_colloc("sc1")
    end
  end

  def sanity_check_on_colloc(colloc_name)
    @collocs[colloc_name].full_clusters.each_value do |cluster|
      if cluster.free_hosts_count != 0 || (cluster.busy_hosts_count != cluster.hosts_count)
        fail InconsistentStateError, "#{@last_processed_timestamp}: #{cluster_name}
        - #{cluster.free_hosts_count} - #{cluster.busy_hosts_count} - #{cluster.hosts_count}"
      end
    end
  end

  def waiting_jobs_of_colloc(colloc_name)
    count = 0
    @collocs[colloc_name].clusters.each do |cluster_name|
      count += @clusters[cluster_name].waiting_jobs_count
    end
    count
  end

  def waiting_tasks_of_colloc(colloc_name)
    count = 0
    @collocs[colloc_name].clusters.each do |cluster_name|
      count += @clusters[cluster_name].waiting_tasks_count
    end
    count
  end

  def timestamp_changed(timestamp)
    if @last_processed_timestamp.nil?
      @creation_time = timestamp - @granularity
      @last_processed_timestamp = @creation_time
    elsif (timestamp - @last_processed_timestamp) > @granularity
      @last_processed_timestamp = timestamp - @granularity
    end
  end

  def process_eviction_event(scheduling_event)    
    @hosts[scheduling_event.host_name].evicted_signal(scheduling_event.timestamp)
  end

  def process_provision_event(scheduling_event)
    host = @hosts[scheduling_event.host_name]
    cluster = @clusters[scheduling_event.cluster_name]
    host.provisioned_signal(scheduling_event.timestamp, cluster)
  end

  def get_collocs_names
    @collocs.keys.join(', ')
  end

  def get_clusters_names
    @clusters.keys.join(', ')
  end

  def get_tasks_map_with_key(job_host_key)
    @tasks_map[job_host_key]
  end

  def get_hosts_from_cluster(cluster_name, status)
    @clusters[cluster_name].get_hosts(status)
  end

  def get_hosts_from_colloc(colloc_name, status)
    @collocs[colloc_name].get_hosts(status)
  end

  def deregister_cluster(cluster)
    @clusters.delete(cluster.name)
  end

  def deregister_colloc(colloc)
    @collocs.delete(colloc.name)
  end

  def deregister_host_from_world(host)
    @hosts.delete[host.name]
  end

  def hosts_count
    @hosts.size
  end

  def increase_running_tasks(count)
    @running_tasks_count += count
  end

  def decrease_running_tasks(count)
    @running_tasks_count -= count
  end

  def increase_waiting_tasks(count)
    @waiting_tasks_count += count
  end

  def decrease_waiting_tasks(count)
    @waiting_tasks_count -= count
  end

  # Mimics Altiscale datacenters behavior owning a number of clusters with host assigned to them
  # and also a number of inventory hosts. Scheduling shouldn't be done between different collocs
  # so it also servers as the logical separation for scheduling.
  # Keeps track of the free/busy hosts and clusters running on capacity along with stats
  # related to their utilization
  # Provides the interface to the scheduler so the scheduler can build its models by knowing at
  # each time what are the available free hosts and what are the clusters that utilize
  # all of their resources
  class Colloc
    attr_reader :full_clusters,
    :inventory_hosts,
    :offline_hosts,
    :free_hosts,
    :busy_hosts,
    :inventory_hosts,
    :transitioning_hosts
    
    attr_accessor :name,
    :clusters,
    :avg_utilization,
    :ticks
    def initialize(world, name)
      @name = name
      @free_hosts = Hash.new
      @busy_hosts = Hash.new
      @full_clusters = Hash.new
      @clusters = Hash.new
      @total_utilization = 0.0
      @ticks = 1 #The number of timestamps/minutes passed since colloc existance
      world.register_colloc(self) ##Let the world know a new colloc exists!
      @inventory_hosts = Hash.new
      @inventory_hosts = create_inventory_hosts(world)
      @offline_hosts = Hash.new
      @transitioning_hosts = Hash.new
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::ERROR
    end

    def create_stats(_timestamp)
      return unless all_hosts_count > 0
      @ticks += 1
      @total_utilization += current_utilization
      avg_utilization
    end

    def print_stats
      puts "Hosts (free | busy | inventory | in_transit):\
      #{free_hosts_count} | #{busy_hosts_count} | #{inventory_hosts_count} | #{transitioning_hosts_count}"
      puts "Utilization (current - avg): \
      #{current_utilization} - #{avg_utilization}"
      # sleep 1
    end

    def avg_utilization
      @total_utilization / @ticks
    end

    def current_utilization
      (free_hosts_count + busy_hosts_count > 0) ? busy_hosts_count.to_f / (free_hosts_count + busy_hosts_count) : 'UNDEF'
    end

    def moving_from_inventory(timestamp, host_names)
      return if host_names.nil? || host_names.empty?
      host_names.each do |host_name|
        @logger.debug "#{timestamp}: Moving #{host_name} from inventory!"
        host = @inventory_hosts.delete(host_name)
        host.in_transit_signal(timestamp)
        @transitioning_hosts[host_name] = host
      end
    end

    def moving_to_inventory(timestamp, host_names)
      return if host_names.nil? || host_names.empty?
      host_names.each do |host_name|
        @logger.debug "Colloc received signal to move: #{host_name} to inventory"
        host = @free_hosts.delete(host_name)
        fail MovingBusyHostError, "Moving a busy host" if host.nil?
        @clusters[host.cluster_name].deregister_host(host_name)
        host.in_transit_signal(timestamp)
        @transitioning_hosts[host_name] = host
      end
    end

    def stdev_utilization
      # TODO: Implement
    end

    def clusters_count
      @clusters.size
    end

    def full_clusters_count
      @full_clusters.size
    end

    def all_hosts_count
      (free_hosts_count + busy_hosts_count)
    end

    def free_hosts_count
      @free_hosts.size
    end

    def busy_hosts_count
      @busy_hosts.size
    end

    def inventory_hosts_count
      @inventory_hosts.size
    end

    def offline_hosts_count
      @offline_hosts.size
    end

    def transitioning_hosts_count
      @transitioning_hosts.size
    end

    def get_free_hosts
      get_hosts(Constants::HostStatus::FREE)
    end

    def get_busy_hosts
      get_hosts(Constants::HostStatus::BUSY)
    end

    def get_hosts(status)
      if (status == Constants::HostStatus::FREE)
        @free_hosts
      elsif (status == Constants::HostStatus::BUSY)
        @busy_hosts
      elsif (status == Constants::HostStatus::INVENTORY)
        @inventory_hosts
      elsif (status == Constants::HostStatus::OFFLINE)
        @offline_hosts
      else
        raise UnsupportedHostStatusError, "Host status unknown or unsupported!"
      end
    end

    def cluster_status_changed_signal(cluster)
      (cluster.full)? add_to_full_clusters(cluster) : remove_from_full_clusters(cluster)
    end

    def host_status_changed_signal(host, prev_status, current_status)
      # Signal received from Host
      @logger.debug "Signal on #{name}: #{host.name} - #{prev_status} --> #{current_status}"
      host_status_updated(host, prev_status, current_status)
    end

    def print_summary
      puts "--> Colloc: #{name}"
      puts "- Hosts: Busy(#{busy_hosts_count}) - Free(#{free_hosts_count})- Inventory(#{inventory_hosts_count})"
    end

    def print_all
      print_summary
      puts 'Free hosts:'
      print_hosts(@free_hosts)
      puts "Busy hosts:"
      print_hosts(@busy_hosts)
      puts 'Inventory hosts:'
      print_hosts(@inventory_hosts)
      puts 'In transit hosts:'
      print_hosts(@transitioning_hosts)

      puts
      print_stats
    end

    def register_cluster(cluster)
      add_to_clusters(cluster)
      add_to_full_clusters(cluster) if cluster.full
    end

    def register_host(host)
      case host.status
      when Constants::HostStatus::INVENTORY
        add_to_inventory_hosts(host)
      when Constants::HostStatus::FREE
        add_to_free_hosts(host)
      else
        add_to_busy_hosts(host)

      end

      if is_on_free_hosts(host) && is_on_busy_hosts(host)
        fail InconsistentStateError, "Registered host #{host.name} in both free and busy sets!"
      end
    end

    # Host is tightly coupled with colloc (its part of the hostname)
    # No need to provide a deregister method

    ################################

    private # all methods that follow will be made private: not accessible for outside objects

    def create_inventory_hosts(world)
      return if world.nil? || world.inventory_hosts_count.nil? || world.inventory_hosts_count == 0

      (1..world.inventory_hosts_count).each do |seq|
        host_name = 'inv' + seq.to_s + '.' + @name + '.verticloud.com'
        host = Host.new(world, self, host_name)
        add_to_inventory_hosts(host)
      end
      @inventory_hosts
    end

    def host_status_updated(host, prev_status, current_status)
      case prev_status
      when Constants::HostStatus::OFFLINE
        remove_from_hosts(host.name, @offline_hosts)
      when Constants::HostStatus::INVENTORY
        remove_from_hosts(host.name, @inventory_hosts)
      when Constants::HostStatus::FREE
        remove_from_hosts(host.name, @free_hosts)
      when Constants::HostStatus::BUSY
        remove_from_hosts(host.name, @busy_hosts)
      when Constants::HostStatus::MOVING
        remove_from_hosts(host.name, @transitioning_hosts)
      else
        fail UnsupportedHostStatusError, 
          "#{host.name} status #{prev_status} not supported on colloc!"
      end

      case current_status
      when Constants::HostStatus::OFFLINE
        add_to_hosts(host, @offline_hosts)
      when Constants::HostStatus::INVENTORY
        add_to_hosts(host, @inventory_hosts)
      when Constants::HostStatus::FREE
        add_to_hosts(host, @free_hosts)
      when Constants::HostStatus::BUSY
        add_to_hosts(host, @busy_hosts)
      when Constants::HostStatus::MOVING
        add_to_hosts(host, @transitioning_hosts)
      else
        fail UnsupportedHostStatusError, 
          "#{host.name} status #{prev_status} not supported on colloc!"
      end
    end

    def remove_from_hosts(host_name, hosts)
      hosts.delete(host_name)
    end

    def add_to_hosts(host, hosts)
      hosts[host.name] = host
    end

    def add_to_clusters(cluster)
      @clusters[cluster.name] = cluster
    end

    def add_to_full_clusters(cluster)
      @full_clusters[cluster.name] = cluster
    end

    def remove_from_full_clusters(cluster)
      @full_clusters.delete(cluster.name)
    end

    def add_to_inventory_hosts(host)
      @inventory_hosts[host.name] = host
    end

    def remove_from_inventory_hosts(host)
      @inventory_hosts.delete(host.name)
    end

    def add_to_free_hosts(host)
      @free_hosts[host.name] = host
    end

    def remove_from_free_hosts(host)
      @free_hosts.delete(host.name)
    end

    def add_to_busy_hosts(host)
      @busy_hosts[host.name] = host
    end

    def remove_from_busy_hosts(host)
      @busy_hosts.delete(host.name)
    end

    def is_on_free_hosts(host)
      @free_hosts.include?(host.name)
    end

    def is_on_busy_hosts(host)
      @busy_hosts.include?(host.name)
    end

    def print_hosts(hosts)
      if hosts.nil? || hosts.empty?
        puts 'None!'
      else
        hosts.each_key { |k| puts k }
      end
    end
  end

  # Mimics YARN cluster behavior. Keeps track of the submitted jobs and their statuses,
  # waiting/ running tasks, and free/busy hosts.
  class Cluster
    # Either keep free/busy as sets containing only host_names or completely remove hosts hash
    attr_accessor :name,
    :colloc_name,
    :hosts,
    :jobs,
    :busy_hosts,
    :offline_hosts,
    :free_hosts,
    :task_status_counters,
    :full, # true if free_hosts is empty and cluster has at least one waiting task
    :provisioned_hosts,
    :yarn_scheduler
    def initialize(world, name, colloc)
      @name = name
      @colloc = colloc
      @hosts = {}
      @free_hosts = {}
      @busy_hosts = {}
      @provisioned_hosts = {} # Keeps track of the hosts given to this cluster by the scheduler
      @offline_hosts = {}  # Obsolete
      @last_seen_hosts = {} # Obsolete - Used to calculate offline hosts
      @jobs = {}
      @job_status_counters = JobStatusCounters.new(@name) # ex: <RUNNING, 10>, <WAITING, 20>...
      @task_status_counters = TaskStatusCounters.new('cluster', @name)
      @full = is_full?
      world.register_cluster(self, colloc.name) # Let the world know a new cluster was born!

      @yarn_scheduler = YarnScheduler.new(self) # Used to assign tasks to additional resources

      @logger = Logger.new(STDOUT)
      @logger.level = Logger::INFO
    end

    def update(event, task_diff)
      fail ArgumentError, "Event not targeting this cluster (Found: #{event.cluster_name} \
      - Expecting: #{@name})" unless event.cluster_name == @name

      # No need to register job to host. Job will be be added in running jobs of host on host.update
      task_status = event.task_status
      begin
        # Add diff calculated for the status of this specific Job to the total count for
        # this status among all jobs on this cluster
        @task_status_counters.add_value_to_status(task_status, task_diff)
      rescue => e
        @logger.error "Tasks on cluster cannot be less than zero! \
        Found: #{@task_status_counters.get_count_of_status(task_status)} for cluster: #{@name}"
        @logger.error 'Cluster object status:'
        @logger.error "#{inspect}"
        @logger.error e.message
        e.backtrace.each { |line| @logger.error line }
        @logger.error e.backtrace
        raise
      end

      # Not accounting hosts in REQUESTED state as seen is very aggressive but eliminates
      # the inconsistency of having waiting tasks while having "free" hosts
      unless event.task_status == Constants::TaskStatus::REQUESTED
        @last_seen_hosts[event.host_name] = event.timestamp
      end

      if busy_hosts_count > 0 && @job_status_counters.running_count == 0
        @logger.error 'Busy Hosts:'
        @busy_hosts.each_key { |k| puts k } if @logger.error?
        @logger.error 'job counters'
        @job_status_counters.print
        @logger.error 'task counters'
        @task_status_counters.print
        fail InconsistentStateError, \
        "Cluster #{@name} - timestamp: #{event.timestamp}: Busy hosts = #{busy_hosts_count} \
      	and running job status counters=#{@job_status_counters.running_count}"
      end
      # Need jobs counters and free/busy hosts to be updated? NO...
      # 1) job_status_counters are updated through job_status_changed_signal
      # 2) host status is updated with host_status_changed_signal
    end

    def register_job(job)
      fail StandardError, "Job with id: #{job.id} already exists!" if has_job(job.id)
      add_job(job)
      @job_status_counters.increase_count_of_status(job.status)
      # Job will be registered with host on cluster update
    end

    def timestamp_processed_signal(timestamp)
      @last_processed_timestamp = timestamp - Constants::Numeric::GRANULARITY
      send_cluster_status_changed_signal if cluster_status_changed?
      # check_for_offline_hosts
      puts "\n" if @logger.debug?
      @logger.debug "#{@name} completed processing of: #{@last_processed_timestamp}"
      print(false, true, true, true) if @logger.debug?
      end_of_timestamp_sanity_checks(timestamp)
      @last_seen_hosts = Hash.new
    end

    def host_status_changed_signal(host, prev_status, current_status)
      # Signal received from Host
      @logger.debug "Signal on #{name}: #{host.name} - #{prev_status} --> #{current_status}"
      host_status_updated(host, prev_status, current_status)
    end

    def task_status_update_signal(host_name, job_id, task_status, diff)

      @task_status_counters.add_value_to_status(task_status, diff)
      send_cluster_status_changed_signal if cluster_status_changed?
      @jobs[job_id].task_status_update_signal(task_status, diff)
      unless (task_status == Constants::TaskStatus::REQUESTED)
        @hosts[host_name].task_status_update_signal(task_status, diff)
      end
    end

    def job_done_on_host_signal(timestamp, job_id, host_name, job_on_host_task_status_counters)
      # Signal received from World

      @logger.debug "job_done_on_host_signal: #{@name} - #{job_id} - #{host_name}!"
      update_task_status_counters(job_on_host_task_status_counters.map)

      @jobs[job_id].done_on_host_signal(host_name, job_on_host_task_status_counters.map)

      # Job not necessarily in running state on host. Host unaware of a job in "REQUESTED" state
      if (@hosts.has_key?(host_name)) # Host can be already de-registered
        @hosts[host_name].job_not_running_signal(timestamp, job_id, job_on_host_task_status_counters.map)
      end
    end

    def job_finished_signal(job_id)
      # Signal received from World
      initial_value = @job_status_counters.get_count_of_status(Constants::JobStatus::FINISHED)

      @logger.debug "job_finished_signal: #{@name} - #{job_id}!"
      job = @jobs[job_id]
      prev_status = job.status # Job status not updated yet - Cluster will trigger done_on_cluster_signal of job later
      new_status = Constants::JobStatus::FINISHED
      update_job_status_counters(prev_status, new_status)

      # Important not to update task_status_counters. Counters are already updated through the
      # job_done_on_host signals
      # update_task_status_counters(job.task_status_counters.map)

      # @task_status_counters.reset_counters unless @job_status_counters.active_on_cluster_count > 0
      send_cluster_status_changed_signal if cluster_status_changed?

      job.done_on_cluster_signal # Send signal to affected job

      final_value = @job_status_counters.get_count_of_status(Constants::JobStatus::FINISHED)
      if(final_value != (initial_value + 1))
        fail InconsistentStateError, "Expected: #{initial_value + 1} - Found: #{final_value}"
      end
      # No need to send signal to each host. Will be updated from job_not_running signals
    end

    def job_status_changed_signal(old_status, new_status)
      # We deal with FINISHED jobs through job_finished_signal only!
      return unless new_status != Constants::JobStatus::FINISHED
      # Signal received from Job
      update_job_status_counters(old_status, new_status)
    end

    def register_host(host, provisioned = false)
      if(host.status == Constants::HostStatus::INVENTORY || host.status == Constants::HostStatus::OFFLINE)
        # A host in inventory shouldn't be registered with a cluster
        fail InconsistentStateError, "Host #{host.name} status is INVENTORY or OFFLINE \
      	                              but tried to register with cluster #{name}"
      end
      @logger.debug "#{@name} registering #{host.name}"
      (host.status == Constants::HostStatus::FREE) ? add_to_free_hosts(host) : add_to_busy_hosts(host)
      @hosts[host.name] = host
      if (provisioned) then @provisioned_hosts[host.name] = host end
      send_cluster_status_changed_signal if cluster_status_changed?
      if (is_on_free_hosts(host.name) && is_on_busy_hosts(host.name))
        fail InconsistentStateError, "Registered host #{host.name} in both free and busy lists!"
      end
    end

    def deregister_host(host_name)
      unless has_host(host_name)
        fail HostNotInCluster, "Host: #{host_name} doesn't exist on #{name}!"
      end
      @logger.debug "#{@last_processed_timestamp}:#{@name} de-registering #{host_name}"
      is_on_free_hosts(host_name)? remove_from_free_hosts(host_name) : remove_from_busy_hosts(host_name)
      is_on_provisioned_hosts(host_name)? remove_from_provisioned_hosts(host_name) : nil
      @hosts.delete(host_name)
      send_cluster_status_changed_signal if cluster_status_changed?
      if @free_hosts.key?(host_name)
        fail InconsistentStateError, "Deleted host #{host_name} still in free list!"
      end
      if @busy_hosts.key?(host_name)
        fail InconsistentStateError, "Deleted host #{host_name} still in busy list!"
      end
    end

    def free_memory
      @memory_capacity - @memory_used
    end

    def has_job(job_id)
      @jobs.key?(job_id)
    end

    def get_job(job_id)
      @jobs[job_id]
    end

    def waiting_tasks_count
      @task_status_counters.waiting_count
    end

    def active_tasks_count
      @task_status_counters.active_count
    end

    def print_task_status_counters
      @task_status_counters.print
    end

    def waiting_jobs_count
      @job_status_counters.waiting_count
    end

    def active_jobs_count
      @job_status_counters.active_on_cluster_count
    end

    def running_jobs_count
      @job_status_counters.running_count
    end

    def print_job_status_counters
      @job_status_counters.print
    end

    def free_hosts_count
      @free_hosts.size
    end

    def busy_hosts_count
      @busy_hosts.size
    end

    def offline_hosts_count
      @offline_hosts.size
    end

    def provisioned_hosts_count
      @provisioned_hosts.size
    end

    def hosts_count
      @hosts.size
    end

    def print_all
      print(true, true, true, true)
    end

    def print_summary
      print(false, false, false)
    end

    def print(host_stats_on = false, job_stats_on = false, task_stats_on = false, display_hosts = false)
      status = (full) ? 'full' : 'has space'
      puts "---> Cluster: #{name} - Status: #{status}"
      puts "- Hosts: Busy(#{busy_hosts_count}) - Free(#{free_hosts_count}) - Prov(#{provisioned_hosts_count})"
      puts "- Tasks Waiting: #{waiting_tasks_count}"
      if display_hosts
        puts 'Busy Hosts:'
        busy_hosts = get_busy_hosts_names
        puts((busy_hosts.empty?) ? 'None!' : busy_hosts.join(','))
        puts 'Free Hosts:'
        free_hosts = get_free_hosts_names
        puts((free_hosts.empty?) ? 'None!' : free_hosts.join(','))
        puts 'Provisioned Hosts:'
        provisioned_hosts = get_provisioned_hosts_names
        puts((provisioned_hosts.empty?) ? 'None!' : provisioned_hosts.join(','))
      end

      print_hosts_stats if host_stats_on

      puts "-- Jobs: Waiting(#{waiting_jobs_count}) - Active(#{active_jobs_count})"
      print_job_status_counters if job_stats_on

      puts "--- Tasks: Waiting(#{waiting_tasks_count}) - Active(#{active_tasks_count})"
      print_task_status_counters if task_stats_on
    end

    def print_hosts_stats
      puts "------------- Stats per Host for Cluster #{@name} -----------------------"
      @hosts.each do |name, host|
        puts "---> Host: #{name} - status: #{host.status}"
        host.print_stats(false, false)
      end
    end

    ################################

    private # all methods that follow will be made private: not accessible for outside objects

    def is_job_running_on_host(finished_job_task_status_counters)
      finished_job_task_status_counters.active_count > 0
    end

    def update_task_status_counters(finished_job_task_status_counters_map)
      @logger.level = Logger::INFO
      @logger.debug "#{@name}: update_task_status_counters..."
      @logger.debug "Before:"
      @task_status_counters.print() if @logger.debug?

      @logger.debug "Updating with:"
      @logger.debug finished_job_task_status_counters_map.inspect

      done = @task_status_counters.get_count_of_status(Constants::TaskStatus::DONE)
      finished_job_task_status_counters_map.each do |k, v|
        if (k != Constants::TaskStatus::DONE)
          done += v
          @task_status_counters.add_value_to_status(k, -v)
        end
      end
      @task_status_counters.set_count_of_status(Constants::TaskStatus::DONE, done)

      @logger.debug "After:"
      @task_status_counters.print() if @logger.debug?

      send_cluster_status_changed_signal if cluster_status_changed?
    end

    def update_job_status_counters(prev_status, new_status)
      return unless prev_status != new_status
      @logger.debug "Job status changed FROM: #{prev_status} TO: #{new_status}"
      begin
        @job_status_counters.update_counters(prev_status, new_status)
      rescue => e
        @logger.error e.message
        e.backtrace.each { |line| @logger.error line }
        raise
      end
    end

    def check_for_offline_hosts
      # * Method not used as hosts can be free while the cluster has tasks waiting because
      # of the way queues work on the capacity scheduler.

      # Every time the timestamp changes cluster checks for dead hosts
      # The most possible case of a host not appearing when a cluster has tasks in waiting
      # status is a manual eviction. Nevertheless, there might be other cases that this occurs
      # like a temporal unavailability of the host.
      # In any case the host will re-register with the cluster next time it appears

      if (@full and free_hosts_count > 0)
        @free_hosts.each_key do |host_name|
          @logger.debug "#{@last_processed_timestamp}: #{@name} checking if #{host_name} is alive!"
          # Consider lost if host doesn't appear for "timeout" timestamps in a row in a status other
          # than requested
          granularity = Constants::Numeric::GRANULARITY
          # Timeout is the number of timestamps that a cluster tolerates no communication
          # with the host before considering it lost
          timeout = 1
          unless (@last_seen_hosts.has_key?(host_name)\
          && (@last_processed_timestamp - @last_seen_hosts[host_name]) < timeout * granularity)
            @logger.debug "#{@last_processed_timestamp}: Host #{host_name} is lost from #{@name}!"
            # sleep 2
            @hosts[host_name].host_is_lost_signal(@last_processed_timestamp)
          end
          # No need to call deregister_host(host_name) - Host will trigger this!
        end
      end
    end

    def host_status_updated(host, prev_status, current_status)
      
      # Cluster doesn't keep track of inventory or moving hosts
      # Do nothing for INVENTORY AND MOVING statuses
      case prev_status
      when Constants::HostStatus::OFFLINE
        remove_from_hosts(host.name, @offline_hosts)
      when (Constants::HostStatus::INVENTORY)
        return
      when Constants::HostStatus::MOVING
        puts "Moving"
        return  
      when Constants::HostStatus::FREE
        remove_from_hosts(host.name, @free_hosts)
      when Constants::HostStatus::BUSY
        remove_from_hosts(host.name, @busy_hosts)
      else
        fail UnsupportedHostStatusError,  
          "#{host.name} status #{prev_status} not supported on cluster!"
      end

      case current_status
      when Constants::HostStatus::OFFLINE
        add_to_hosts(host, @offline_hosts)
      when Constants::HostStatus::INVENTORY
        return
      when Constants::HostStatus::MOVING
        return
      when Constants::HostStatus::FREE
        add_to_hosts(host, @free_hosts)
      when Constants::HostStatus::BUSY
        add_to_hosts(host, @busy_hosts)
      else
        fail UnsupportedHostStatusError,  
          "#{host.name} status #{current_status} not supported on cluster!"
      end
    end

    def remove_from_hosts(host_name, hosts)
      hosts.delete(host_name)
    end

    def add_to_hosts(host, hosts)
      hosts[host.name] = host
    end

    def cluster_status_changed?
      prev_status = @full
      @full = is_full?
      (@full != prev_status)
    end

    def is_full?
      # Should be called only at the end of a timestamp, after all events are processed
      # In between state on cluster is inconsistent
      # because the input doesn't contain explicit information of when a host goes back to inventory
      # leading in cases that the cluster has waiting tasks but it doesn't appear as using all its
      # hosts. In fact it does use all its hosts but some of them are still appearing as registered
      # and unused since we didn't de-register them when they went back to inventory. The will be
      # de-registered only when and if they are assigned on a different cluster.
      (waiting_tasks_count > 0)? true : false
      # (free_hosts_count == 0 && waiting_tasks_count > 0) # --> This can lead to inconsistencies
    end

    def send_cluster_status_changed_signal
      @colloc.cluster_status_changed_signal(self)
    end

    def is_on_provisioned_hosts(host_name)
      @provisioned_hosts.key?(host_name)
    end
    def is_on_free_hosts(host_name)
      @free_hosts.key?(host_name)
    end

    def is_on_busy_hosts(host_name)
      @busy_hosts.key?(host_name)
    end

    def is_on_registered_hosts(host_name)
      @hosts.key?(host_name)
    end

    def end_of_timestamp_sanity_checks(timestamp)
      # Checks should only be performed after all events of a timestamp are processed
      if (@job_status_counters.running_count == 0 && busy_hosts_count > 0) \
      || (@job_status_counters.running_count > 0 && busy_hosts_count == 0)
        fail InconsistentStateError, "#{timestamp}: #{@name}
                                  - Running job counters: #{@job_status_counters.running_count}
                                    but busy hosts count: #{busy_hosts_count}"
      end

      unless free_hosts_count > 0 || busy_hosts_count > 0 || offline_hosts_count > 0
        @logger.warn "#{timestamp}: No Free, Busy or Offline hosts on #{@name}!"
      end

    end

    def add_job(job)
      @jobs[job.id] = job
    end

    def add_to_free_hosts(host)
      @free_hosts[host.name] = host
    end

    def remove_from_provisioned_hosts(host_name)
      @provisioned_hosts.delete(host_name)
    end
    
    def remove_from_free_hosts(host_name)
      @free_hosts.delete(host_name)
    end

    def add_to_busy_hosts(host)
      @busy_hosts[host.name] = host
    end

    def remove_from_busy_hosts(host_name)
      @busy_hosts.delete(host_name)
    end

    def get_host(host_name)
      @hosts[host_name]
    end

    def has_host(host_name)
      @hosts.key?(host_name)
    end

    def get_hosts(status)
      if (status == Constants::HostStatus::FREE)
        @free_hosts
      else
        @busy_hosts
      end
    end

    def get_free_hosts_names
      @free_hosts.keys
    end

    def get_busy_hosts_names
      @busy_hosts.keys
    end

    def get_provisioned_hosts_names
      @provisioned_hosts.keys
    end

    def get_all_hosts_names
      @hosts.keys
    end
  end

  # Mimics YARNs host. Keeps track of the jobs/tasks running and their corresponding statuses
  class Host

    attr_accessor :name,
    :status,
    :last_processed_timestamp, # Last time object was accessed trough event or signal
    :cluster_name,
    :colloc_name,
    :task_status_counters,
    :tasks_capacity, # TODO: Determine how many tasks per host
    :cluster_name,
    :cluster,
    :stats,
    :memory_capacity,
    :provisioned, # Indicates whether the host is provisioned to a cluster
    :memory_used,
    :running_jobs
    def initialize(world, colloc, name)
      @name = name
      @colloc = colloc
      @status = Constants::HostStatus::INVENTORY
      # TODO: Get actual memory per host from file
      @memory_capacity = Constants::ResourceValues::MEMORY_CAPACITY # Memory in MB
      # TODO: Decrease accordingly for all running tasks
      # Currently used only for the provisioned hosts
      @memory_used = 0
      @provisioned = false # Defines that a host is added as extra resource to a cluster
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::INFO
      world.register_host(self, colloc.name) # Let the world know a new host was added!
    end

    def configure(timestamp, cluster, task_status = nil)
      @cluster = cluster
      @cluster_name = cluster.name

      prev_status = @status
      if (@status == Constants::HostStatus::INVENTORY || @status == Constants::HostStatus::OFFLINE)
        if (task_status == Constants::TaskStatus::REQUESTED)
          @status = Constants::HostStatus::FREE
        else
          @status = Constants::HostStatus::BUSY
        end
      elsif(@status == Constants::HostStatus::MOVING && task_status.nil?)
        @status = Constants::HostStatus::FREE
        @provisioned = true
        @logger.debug "#{timestamp}: Provision of #{name} to #{cluster.name} completed!"
      else
        @logger.debug "Problem configuring #{name} with status #{@status} to #{cluster.name}"
      end
      @cluster.register_host(self, @provisioned)
      @colloc.host_status_changed_signal(self, prev_status, @status)

      @task_status_counters = TaskStatusCounters.new('host', @name)
      @running_jobs = Set.new # Job has tasks in any status other than "REQUESTED" in past minute
      @finished_jobs = Set.new # Job had ran on this host but its now done on the cluster
      @last_processed_timestamp = timestamp
      @stats = HostStats.new(name, timestamp, @status)
    end

    def update(event, task_diff, cluster)
      if (event.host_name != @name)
        fail ArgumentError, "Event not targeting this Host (found: #{event.host_name} \
      - expecting: #{@name})"
      end

      if(event.timestamp < @last_processed_timestamp)
        fail MonotonicTimeIncreaseError, "Cannot go back in time! Signal time received: \
      	#{event.timestamp} - Last update: #{@last_processed_timestamp}"
      end
      @last_processed_timestamp = event.timestamp

      @logger.debug "task_diff = #{task_diff}"

      if (event.cluster_name != @cluster_name)
        move_to_cluster(event.timestamp, cluster) 
      end # Host changed cluster!

      # Ignore events on REQUESTED state - Host is not aware of them in reality.
      if (event.task_status == Constants::TaskStatus::REQUESTED) then return end

      if (event.task_status == Constants::TaskStatus::RUNNING)
        # TODO: Instead of using the contants avg_task_mem the avg_task_mem for this particular
        # job should be used. Need to have access to jobs from host - not just job_ids or have
        # access to another class that keeps the mappings
        update_memory(task_diff, Constants::ResourceValues::AVG_TASK_MEM)
      end

      # job_ids are unique for the lifetime of particular cluster.
      add_job_to_running(event.job_id) unless runs_job?(event.job_id)

      task_status = event.task_status
      begin
        @task_status_counters.add_value_to_status(task_status, task_diff)
      rescue => e
        @logger.error 'Tasks on host cannot be less than zero!'
        @logger.error 'Host object status:'
        @logger.error "#{inspect}"
        @logger.error 'TaskCount BEFORE event:'
        @logger.error @task_status_counters.get_owner
        @task_status_counters.print
        @logger.error e.backtrace
        raise
      end

      print_stats(true, true) if @logger.debug?
    end

    def timestamp_processed_signal(timestamp)
      update_status
    end

    def task_status_update_signal(task_status, diff)
      return if task_status == Constants::TaskStatus::REQUESTED
      if (task_status == Constants::TaskStatus::RUNNING)
        update_memory(diff, Constants::ResourceValues::AVG_TASK_MEM)
      end
      @task_status_counters.add_value_to_status(task_status, diff)
    end

    def in_transit_signal(timestamp)
      # Host is moved from/ to inventory
      reset(timestamp, Constants::HostStatus::MOVING)

    end

    def provisioned_signal(timestamp, cluster)
      self.configure(timestamp, cluster)
    end

    def host_is_lost_signal(timestamp)
      # *Not used
      prev_status = @status
      reset(timestamp, Constants::HostStatus::OFFLINE)
      @colloc.host_status_changed_signal(self, prev_status, @status)
      @cluster.deregister_host(@name)
    end

    def evicted_signal(timestamp)
      prev_status = @status
      reset(timestamp, Constants::HostStatus::INVENTORY)
      @colloc.host_status_changed_signal(self, prev_status, @status)
      # Already de-registered with cluster
      @logger.debug "#{timestamp}:#{name} received eviction signal!"
      # TODO: Implement
    end
    
    def reset(timestamp, status)
      # Update other members
      @task_status_counters = TaskStatusCounters.new('host', @name)
      @running_jobs = Set.new
      @finished_jobs = Set.new
      @status = status
      @last_processed_timestamp = timestamp
      @stats = HostStats.new(@name, timestamp, @status)
    end

    def job_not_running_signal(timestamp, job_id, job_on_host_task_status_counters_map)
      return unless runs_job?(job_id)
      # Received from Cluster
      # This signal doesn't imply that job finished on this host forever.
      # New tasks of this job might be assigned to this host later
      if (timestamp < @last_processed_timestamp)
        fail MonotonicTimeIncreaseError, "Cannot go back in time! Signal time received:
      	  #{timestamp} - Last update: #{last_processed_timestamp}"
      end
      @last_processed_timestamp = timestamp

      @logger.level = Logger::INFO
      @logger.debug "#{last_processed_timestamp} - #{name}: job_not_running_signal..."
      update_task_status_counters(job_on_host_task_status_counters_map)
      remove_job_from_running(job_id) # Will also update host status after removing the job
      # Job stopped running just on host not necessarily finished on cluster -
      # Appropriate signal will be send on the job from the cluster
    end

    def free_memory
      @memory_capacity - @memory_used
    end

    def print_stats(task_stats_on = false, _force_update = false)
      # Attention: Forcing updates leads to inconsistencies
      # (force_update)? @stats.update_and_print(@last_processed_timestamp) : @stats.print
      @stats.update_and_print(@last_processed_timestamp)
      print_task_counters if task_stats_on
    end

    def get_updated_stats(timestamp)
      @stats.create_stats(timestamp)
      @stats
    end

    def runs_job?(job_id)
      @running_jobs.include?(job_id)
    end

    ################################

    private # all methods that follow will be made private: not accessible for outside objects

    def update_memory(running_tasks, avg_task_mem)
      @memory_used += (running_tasks * avg_task_mem)
      # Avg used but should never exit maximum capacity or get lower than zero
      @memory_used = [0,[@memory_used, @memory_capacity].min].max
    end

    def print_task_counters
      @task_status_counters.print
    end

    def add_job_to_running(job_id)
      fail ArgumentError, "Job with id: #{job_id} already running on #{@name}!" if runs_job?(job_id)
      @logger.debug "INFO: #{job_id} running on #{@name}"
      @running_jobs.add(job_id)
      # update_status is not needed as this method is called only when the host is updated and so
      # the status should be updated once when the timestamp_processed_signal is received
    end

    def remove_job_from_running(job_id)
      delete_job(job_id)
      update_status
    end

    def update_task_status_counters(done_job_on_host_task_status_counters_map)
      # job_on_host_task_status_counters contains the counters of the finished job on this host
      done = @task_status_counters.get_count_of_status(Constants::TaskStatus::DONE)
      done_job_on_host_task_status_counters_map.each do |status, v|
        if status != Constants::TaskStatus::DONE && status != Constants::TaskStatus::REQUESTED
          done += v
          @task_status_counters.add_value_to_status(status, -v)
          if (status == Constants::TaskStatus::RUNNING)
            update_memory(v, Constants::ResourceValues::AVG_TASK_MEM)
          end
        end
      end
      @task_status_counters.set_count_of_status(Constants::TaskStatus::DONE, done)
    end

    def update_status
      prev_status = @status
      @status = (running_jobs_count > 0) ? Constants::HostStatus::BUSY : Constants::HostStatus::FREE
      # Every time the status changes a signal is send to the cluster, colloc and stats
      @logger.debug "#{last_processed_timestamp}: prev: #{prev_status}\
                         - status: #{@status} - #{running_jobs_count}"

      if (prev_status != @status)
        # Design choice: If instead of sending a signal we pass the host object to the
        # interested resources (ex: cluster, colloc) so they can take action, then we should make
        # sure that their update is happening after host's update
        @cluster.host_status_changed_signal(self, prev_status, @status)
        @colloc.host_status_changed_signal(self, prev_status, @status)

        @stats.status_changed_update(@last_processed_timestamp, prev_status, @status)
        # XXX: Memory used should be 0 when no tasks running - Just subtracking avg_task_mem
        # might lead to memory used greater than 0 even when host doesn't run any tasks
        if (@status == Constants::HostStatus::FREE) then @memory_used = 0 end
      end
    end

    def move_to_cluster(timestamp, new_cluster)
      # World object is taking care of de-registering host from its previous cluster and
      # registering to the new one - No need to update_status and send signals.
      # World will send the appropriate signals to clusters
      @logger.info "Host removed from cluster: #{@cluster_name} and assigned to: #{new_cluster.name}!!!"
      @cluster = new_cluster
      @cluster_name = @cluster.name
      reset(timestamp, Constants::HostStatus::FREE)
    end

    def finished_jobs_count
      @finished_jobs.size
    end

    def is_registered_job(job_id)
      runs_job?(job_id) || is_finished_job(job_id)
    end

    def is_finished_job(job_id)
      @finished_jobs.include?(job_id)
    end

    def running_jobs_count
      @running_jobs.size
    end

    def add_job_to_registered(job_id)
      @running_jobs.add(job_id)
    end

    def delete_job(job_id)
      @running_jobs.delete(job_id)
      @finished_jobs.add(job_id)
    end

    class HostStats
      attr_reader :host_name,
      :max_time_busy,
      :max_time_free,
      :min_time_busy,
      :min_time_free,
      :total_time_busy,
      :total_time_free,
      :busy_count,
      :free_count,
      :current_status,
      :last_status_change,
      :granularity
      def initialize(host_name, timestamp, current_status)
        @host_name = host_name
        @max_time_busy = -1
        @max_time_free = -1
        @min_time_busy = -1
        @min_time_free = -1
        @avg_time_busy = -1
        @avg_time_free = -1
        @total_time_busy = 0
        @past_busy_time = 0
        @total_time_free = 0
        @past_free_time = 0
        @busy_count = (current_status == Constants::HostStatus::BUSY)? 1 : 0
        @free_count = (current_status == Constants::HostStatus::FREE)? 1 : 0
        # Total number of times the host changed status including the ones on the same second
        # that we don't consider for stats updates
        @transitions_count = 0
        @current_status = current_status
        @time_in_current_status = 0
        @time_registered = timestamp
        @last_status_change = @time_registered
        @last_stats_refresh = -1
        @granularity = Constants::Numeric::GRANULARITY
        @logger = Logger.new(STDOUT)
        @logger.level = Logger::INFO
      end

      def status_changed_update(current_timestamp, prev_status, current_status)
        return if prev_status == Constants::HostStatus::INVENTORY || 
                  prev_status == Constants::HostStatus::MOVING
                  
        throw InconsistentStateError, 'Host cannot be on the same status' unless prev_status != current_status
        if @logger.debug? then "Stats status changed FROM: #{prev_status} TO: #{current_status}" end
        @transitions_count += 1
        return unless @last_status_change < current_timestamp # If status is changing on the same timestamp there is no point updating the stats

        @logger.debug "#{current_timestamp} - #{host_name}: #{prev_status} -> #{current_status}" 
        (current_status == Constants::HostStatus::FREE) ? host_changed_from_busy_to_free(current_timestamp) : host_changed_from_free_to_busy(current_timestamp)
        @current_status = current_status
        print_all if @logger.debug?
      end

      def create_stats(timestamp)
        create_stats_since_last_status_change(timestamp)
        print_all if @logger.debug?
      end

      def update_and_print(timestamp)
        # Stats are updated automatically every time host status is changed and host stays in the
        # new status for at least a minute. This method forces the update of the stats in order
        # to have the fresher values from the last time the host got on its current state.
        create_stats(timestamp)
        (@logger.debug?) ? print_all : print
      end

      def print_all
        instance_variables.each { |k, _v| puts "#{k}: #{instance_variable_get(k)}" }
      end

      def print
        puts('*Node active less than 1 min') unless @time_registered < (@last_stats_refresh - @granularity)
        puts "Time free: (avg - min - max): (#{avg_time_free_to_s} - #{min_time_free_to_s} - #{max_time_free_to_s})"
        puts "Time busy: (avg - min - max): (#{avg_time_busy_to_s} - #{min_time_busy_to_s} - #{max_time_busy_to_s})"
        total_count = @free_count + @busy_count
        @logger.debug "State transitions: (free - busy - total | skipped): (#{@free_count} \
	- #{@busy_count} - #{total_count} | #{@transitions_count - total_count})"
      end

      private

      # Called after a host status is changed - Can also be called independently to get stats
      # at any particular time. When called without a status change occurring the status on the
      # current timestamp is not taken into consideration. For example if a cluster is running
      # for timestamp 20 and timestamp 80 and the method is called on timestamp 80, the calculated
      # time in current status will be 60 and not 120
      def create_stats_since_last_status_change(current_timestamp)
        return unless @last_stats_refresh < current_timestamp
        @last_stats_refresh = current_timestamp
        elapsed_time = current_timestamp - @last_status_change
        @time_in_current_status = elapsed_time
        @logger.debug "#{current_timestamp} - time_in_current_status: #{@time_in_current_status} \
	(#{@current_status}) - elapsed: #{elapsed_time} - last_change: #{last_status_change}"

        # Averages shouldn't be updated before the status is changed to avoid inconsistencies.
        # Only time they get values is before any transition has happened
        if (@current_status == Constants::HostStatus::FREE)
          @max_time_free = (@time_in_current_status > @max_time_free) ? @time_in_current_status : @max_time_free
          @min_time_free = @time_in_current_status unless @min_time_free > 0
          @avg_time_free = (@avg_time_free > 0) ? @avg_time_free : @time_in_current_status
          @total_time_free = @time_in_current_status + @past_free_time
        else
          @max_time_busy = (@time_in_current_status > @max_time_busy) ? @time_in_current_status : @max_time_busy
          @min_time_busy = @time_in_current_status unless @min_time_busy > 0
          @avg_time_busy = (@avg_time_busy > 0) ? @avg_time_busy : @time_in_current_status
          @total_time_busy = @time_in_current_status + @past_busy_time
        end
      end

      # OPTIMIZE: Make host_changed_from_x_to_y one function for both free and busy statuses
      def host_changed_from_busy_to_free(current_timestamp)
        # The time passed since to detect a finished job and update host status.
        # Its the beginning of the 2nd timestamp that the job is not detected in the logs.
        detection_lag = @granularity

        elapsed_time = current_timestamp - @last_status_change # includes detection lag
        fail ArgumentError, 'Method called for status change on the same timestamp!' unless elapsed_time > 0
        @free_count += 1

        status_changed_time = current_timestamp - detection_lag
        create_stats_since_last_status_change(status_changed_time)
        @last_status_change = status_changed_time
        @avg_time_busy = (@busy_count > 0)? @total_time_busy / @busy_count : -1
        busy_time = elapsed_time - detection_lag
        if busy_time < @min_time_busy || @min_time_busy == -1 then @min_time_busy = busy_time end
        @past_busy_time += busy_time
      end

      def host_changed_from_free_to_busy(current_timestamp)
        detection_lag = @granularity
        elapsed_time = current_timestamp - @last_status_change
        fail ArgumentError, 'Method called for status change on the same timestamp!' unless elapsed_time > 0

        @busy_count += 1

        status_changed_time = current_timestamp - detection_lag
        create_stats_since_last_status_change(status_changed_time)
        @last_status_change = status_changed_time
        @avg_time_free =  (@free_count > 0)? @total_time_free / @free_count : -1
        free_time = elapsed_time - detection_lag
        if free_time < @min_time_free || @min_time_free == -1 then @min_time_free = free_time end
        @past_free_time += free_time
      end

      def min_time_free_to_s
        (@min_time_free == -1) ? '?' : @min_time_free
      end

      def min_time_busy_to_s
        (@min_time_busy == -1) ? '?' : @min_time_busy
      end

      def max_time_free_to_s
        (@max_time_free == -1) ? '?' : @max_time_free
      end

      def max_time_busy_to_s
        (@max_time_busy == -1) ? '?' : @max_time_busy
      end

      def avg_time_busy_to_s
        if (@avg_time_busy == -1)
          s = '?'
        elsif (@avg_time_busy == 0)
          s = "<#{@granularity}"
        else
          s = @avg_time_busy
        end

        s
      end

      def avg_time_free_to_s
        if (@avg_time_free == -1)
          s = '?'
        elsif (@avg_time_free == 0)
          s = "<#{@granularity}"
        else
          s = @avg_time_free
        end

        s
      end
    end
  end

  # Mimics YARN job/application behavior. Each job belongs to a cluster, has a number of tasks on
  # different statuses and runs on a set of hosts. Whenever the running status of a job changed
  # (that is based on the number of tasks on an active or waiting state) the job will send a signal
  # to the cluster it belongs to
  class Job
    attr_accessor :id,
    :status,
    :prev_status,
    :task_status_counters,
    :cluster_name,
    :used_host_names,
    :augmented,
    :avg_task_mem,
    :avg_task_time
    def initialize(cluster, id)
      @id = id
      @cluster = cluster
      @cluster_name = cluster.name
      @used_host_names = Set.new
      @prev_status = Constants::JobStatus::UNDEFINED
      @status = Constants::JobStatus::UNDEFINED
      @task_status_counters = TaskStatusCounters.new('job', @id)
      cluster.register_job(self) # Let the cluster know that a new job was created
      @augmented = false # True when the job is assigned additional resources by the scheduler
      @avg_task_mem = Constants::ResourceValues::AVG_TASK_MEM # TODO: Get actual value from file with per job task mappings
      @avg_task_time = Constants::ResourceValues::AVG_TASK_TIME  # TODO: Get actual value from file with per job task mappings
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::ERROR
    end

    def update(event, task_diff, cluster)
      unless(event.job_id == @id)
        fail ArgumentError, "Event not targeting this job, cluster couple! (found: #{event.job_id}\
      , #{event.cluster_name} - expecting: #{@id}, #{@cluster_name})"
      end

      @used_host_names.add(event.host_name) unless (event.task_status == Constants::TaskStatus::REQUESTED)

      task_status = event.task_status
      status_changed = task_status_updated(task_status, task_diff)
      if status_changed
        # event.print
        cluster.job_status_changed_signal(@prev_status, @status)
      end
      # No need to send signal to the hosts running the job
      # the hosts will register the job_id on their update method
    end

    def timestamp_processed_signal(timestamp)
      # Do nothing
      # TODO: Change job status once inside here and delete from update method
    end

    def task_status_update_signal(task_status, diff)
      @task_status_counters.add_value_to_status(task_status, diff)
      # Don't call update_status. Status will be updated later on update method
    end

    def done_on_cluster_signal
      # Signal received from Cluster
      @logger.debug "Job #{id} received done_on_cluster_signal!"
      return unless @status != Constants::JobStatus::FINISHED # Making sure we do the updates just once
      @status = Constants::JobStatus::FINISHED
      @task_status_counters.reset_counters
      @used_host_names.clear

    end

    def done_on_host_signal(host_name, job_on_host_task_status_counters_map)
      # Signal received from Cluster
      return unless is_using_host(host_name) # Making sure we do the updates just once
      stop_using_host(host_name)
      update_task_status_counters(job_on_host_task_status_counters_map)
      # task_status_containers and status will be updated on next event accordingly.
      # Shouldn't deal with it here!
      # Corner case: What if there is no other update for this job on input?
      # Then done_on_cluster_signal will be send on the next minute and task_status_counters
      # will be reset
    end

    ################################

    private # all methods that follow will be made private: not accessible for outside objects

    def task_status_updated(task_status, diff)
      @task_status_counters.add_value_to_status(task_status, diff)
      update_status
    end

    def update_task_status_counters(done_job_on_host_task_status_counters_map)
      # job_on_host_task_status_counters contains the counters of the finished job on this host
      done = @task_status_counters.get_count_of_status(Constants::TaskStatus::DONE)
      done_job_on_host_task_status_counters_map.each do |k, v|
        if (k != Constants::TaskStatus::DONE)
          done += v
          @task_status_counters.add_value_to_status(k, -v)
        end
      end
      @task_status_counters.set_count_of_status(Constants::TaskStatus::DONE, done)
    end

    def stop_using_host(host_name)
      @used_host_names.delete(host_name)
    end

    def is_using_host(host_name)
      @used_host_names.include?(host_name)
    end

    def update_status
      if @task_status_counters.active_count > 0
        new_status = Constants::JobStatus::RUNNING
      elsif @task_status_counters.waiting_count > 0
        new_status = Constants::JobStatus::WAITING
      elsif (@status != Constants::JobStatus::UNDEFINED)
        # Don't update to finish because this conflicts with job_finished_signal that will be
        # received from cluster and is based on the prev_status of the job to update correctly the
        # counters. The cluster will also send the done_on_cluster_signal to the job so eventually
        # the job will be updated to FINISHED
        new_status = Constants::JobStatus::FINISHED
      else
        new_status = Constants::JobStatus::UNDEFINED
        @logger.warn "Job #{id} has UNDEFINED status!"
      end

      # FINISHED status will be treated with done_on_cluster signal received from cluster.
      return false if (new_status == Constants::JobStatus::FINISHED)

      @prev_status = @status
      @status = new_status

      unless @task_status_counters.get_count_of_status(@status) >= 0
        fail RangeError, "Tasks of job cannot be less than zero!
          (Found: #{@task_status_counters.get_count_of_status(status)} - job: #{@id})"
      end
      @prev_status != @status
    end
  end

  # The key that distinguishes a job on a per cluster basis. Used to update the LRU caches.
  class ClusterJobKey
    attr_reader :cluster_name, :job_id
    def initialize(cluster_name, job_id)
      @cluster_name = cluster_name
      @job_id = job_id
    end

    def eql?(another_key)
      @cluster_name == another_key.cluster_name && @job_id == another_key.job_id
    end

    def hash
      [@cluster_name, @job_id].hash
    end

    def print
      puts "ClusterJobKey: #{@cluster_name} - #{@job_id}"
    end
  end

  # The key that distinguishes a job on a per host basis. Used to update the LRU caches
  # AND to caclulate the task difference between the differen event updates of a particular
  # task status.
  class HostJobKey
    attr_reader :cluster_name, :host_name, :job_id
    def initialize(cluster_name, host_name, job_id)
      @cluster_name = cluster_name
      @host_name = host_name
      @job_id = job_id
    end

    def eql?(another_key)
      @cluster_name == another_key.cluster_name && @host_name == another_key.host_name && @job_id == another_key.job_id
    end

    def hash
      [@cluster_name, @host_name, @job_id].hash
    end

    def print
      puts "HostJobKey: #{@cluster_name} - #{@host_name} - #{@job_id}"
    end
  end

  # The key that distinguishes a task status on a per job and host basis.
  # Used to update the LRU caches.
  class HostJobStatusKey
    attr_reader :cluster_name, :host_name, :job_id, :task_status
    def initialize(cluster_name, host_name, job_id, task_status)
      @cluster_name = cluster_name
      @host_name = host_name
      @job_id = job_id
      @task_status = task_status
    end

    def eql?(another_key)
      @cluster_name == another_key.cluster_name && @host_name == another_key.host_name \
      && @job_id == another_key.job_id && @task_status == another_key.task_status
    end

    def hash
      [@cluster_name, @host_name, @job_id, @task_status].hash
    end

    def print
      puts "HostJobStatusKey: #{@cluster_name} - #{@host_name} - #{@job_id} - #{@task_status}"
    end
  end

  # Tracking the number of jobs on each status. Used for sanity check, create stats and
  # to update status of a cluster between free/busy depending on the number of running/waiting jobs
  class JobStatusCounters
    attr_reader :cluster_name,
    :map
    def initialize(cluster_name)
      @cluster_name = cluster_name
      @map = Hash.new(0)
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::INFO
    end

    def print
      puts "UNDEFINED: #{@map[Constants::JobStatus::UNDEFINED]}"
      puts "WAITING: #{@map[Constants::JobStatus::WAITING]}"
      puts "RUNNING: #{@map[Constants::JobStatus::RUNNING]}"
      puts "FINISHED: #{@map[Constants::JobStatus::FINISHED]}"
    end

    def get_count_of_status(job_status)
      @map[job_status]
    end

    def set_count_of_status(job_status, count)
      @logger.debug "status: #{job_status} of cluster: #{cluster_name} is set to #{count}. Caller: #{caller[0]}"
      unless count >= 0
        fail RangeError, "Job count cannot be less than zero!
          (found: #{count} for status: #{job_status} and cluster: #{@cluster_name})
          - Caller: #{caller[0]}"
      end
      @map[job_status] = count
    end

    def update_counters(old_status, new_status)
      return unless old_status != new_status
      # NOTE: The following sanity check works for almost all cases but there is a chance that
      # a resource manager re-attempts a job with the same job-id but there is not info to
      # distinguish this case in the current dogfood database.
      # unless (old_status != Constants::JobStatus::FINISHED)
      #   raise ArgumentError, "Job status counters can't go back from #{old_status} status to #{new_status} - cluster: #{cluster_name}"
      # end

      begin
        decrease_count_of_status(old_status)
        increase_count_of_status(new_status)
      rescue => e
        @logger.error e.message
        e.backtrace.each { |line| @logger.error line }
        raise
      end
    end

    def increase_count_of_status(job_status)
      add_value_to_status(job_status, 1)
    rescue => e
      @logger.error e.message
      e.backtrace.each { |line| @logger.error line }
      raise
    end

    def decrease_count_of_status(job_status)
      add_value_to_status(job_status, -1)
    rescue => e
      @logger.error e.message
      e.backtrace.each { |line| @logger.error line }
      raise
    end

    # When on waiting status the job is known to the cluster but not on the cluster hosts
    def active_on_cluster_count
      (waiting_count + running_count)
    end

    def waiting_count
      @map[Constants::JobStatus::WAITING]
    end

    def running_count
      @map[Constants::JobStatus::RUNNING]
    end

    private

    def add_value_to_status(job_status, value)
      set_count_of_status(job_status, (get_count_of_status(job_status) + value))
    rescue => e
      @logger.error e.message
      e.backtrace.each { |line| @logger.error line }
      @logger.error print
      @logger.error "status: #{job_status} - count: #{get_count_of_status(job_status)} - adding: #{value}"
      raise
    end
  end

  # Tracking the number of tasks on each status. Clusters, Hosts and Jobs contain
  # TaskStatusCounters and determine their statuses (free, busy etc) and the transitions
  # between statuses from these counters
  class TaskStatusCounters
    attr_reader :owner_type,
    :owner_id,
    :map
    def initialize(owner_type, owner_id)
      @owner_type = owner_type
      @owner_id = owner_id
      @map = Hash.new(0)
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::ERROR
    end

    def print
      puts "REQUESTED: #{@map[Constants::TaskStatus::REQUESTED]}"
      puts "RESERVED: #{@map[Constants::TaskStatus::RESERVED]}"
      puts "ALLOCATED: #{@map[Constants::TaskStatus::ALLOCATED]}"
      puts "ACQUIRED: #{@map[Constants::TaskStatus::ACQUIRED]}"
      puts "RUNNING: #{@map[Constants::TaskStatus::RUNNING]}"
      puts "EXPIRED: #{@map[Constants::TaskStatus::EXPIRED]}"
      puts "DONE: #{@map[Constants::TaskStatus::DONE]}"
    end

    def reset_counters
      # OPTIMIZE: Done state not necessary for calculations - just keeping stats. consider removing.
      @logger.debug "Resetting task counters of type: #{@owner_type} with id #{@owner_id}!!!"
      done = @map[Constants::TaskStatus::DONE]
      @map.each do |k, v|
        if k != Constants::TaskStatus::DONE && k != Constants::TaskStatus::REQUESTED
          done += v
        end
      end
      @map[Constants::TaskStatus::REQUESTED] = 0
      @map[Constants::TaskStatus::RESERVED] = 0
      @map[Constants::TaskStatus::ALLOCATED] = 0
      @map[Constants::TaskStatus::ACQUIRED] = 0
      @map[Constants::TaskStatus::RUNNING] = 0
      @map[Constants::TaskStatus::EXPIRED] = 0
      @map[Constants::TaskStatus::DONE] = done
    end

    def get_owner
      @owner_id
    end

    def get_count_of_status(task_status)
      @map[task_status]
    end

    def set_count_of_status(task_status, count)
      unless count >= 0
        fail RangeError, "Task count cannot be less than zero! (Found: #{count} for status:
      	#{task_status} - Owner type: #{@owner_type} with ID: #{@owner_id})"
      end
      @map[task_status] = count
    end

    def add_value_to_status(task_status, value)
      @logger.debug "Caller: #{caller[0]}"
      @logger.debug "Adding #{value} to status: #{task_status} of #{owner_type} task counters with id: #{owner_id}"
      @logger.debug "Previous value: #{get_count_of_status(task_status)}"
      set_count_of_status(task_status, (get_count_of_status(task_status) + value))
      @logger.debug "Result: #{get_count_of_status(task_status)}"
    rescue => e
      @logger.error "Adding #{value} to status: #{task_status} of #{owner_type} task counters with id: #{owner_id}"
      @logger.error e.message
      e.backtrace.each { |line| @logger.error line }
      raise
    end

    # The events on the input file with status "REQUESTED" are already filtered to be the ones being in REQUESTED state for more than 10 seconds
    def waiting_count
      @map[Constants::TaskStatus::REQUESTED]
    end

    def active_count
      @map[Constants::TaskStatus::RESERVED] + @map[Constants::TaskStatus::ALLOCATED] \
      + @map[Constants::TaskStatus::ACQUIRED] + @map[Constants::TaskStatus::RUNNING]
    end
  end
end
