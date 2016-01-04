require_relative 'events/synthetic_event'
require_relative 'state_updater'
require_relative 'modules/constants'
require_relative 'modules/errors'
require 'logger'

# Mimics a Yarn Scheduler but acts only on tasks that are waiting (REQUESTED) for clusters that
# are provisioned with additional resources.
# From these tasks in REQUESTED status it generates synthetic tasks for the future timestamps
# and until a task is finished and assigns them to the provisioned resources.
# Then it enqueues these synthetic tasks to the state updater so they can be send back to the world
# model and be treated similar to the original input_log_events that indicate a task in RUNNING
# status
class YarnScheduler

  attr_reader :cluster,
  :synthetic_events,
  :augmented_jobs_on_hosts
  def initialize(cluster)
    @cluster = cluster

    @gener_events_map = {} # <run_event, task_count>
    @augmented_jobs_on_hosts = {}  # <host_job_key, assigned_containers>
    @avg_task_mem = Constants::ResourceValues::AVG_TASK_MEM # TODO(stratos): Get actual from file for this job_id
    @avg_task_time = Constants::ResourceValues::AVG_TASK_TIME # TODO(stratos): Get actual from file for this job_id
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::INFO
  end

  def augments_job_on_host(host_job_key)
    @augmented_jobs_on_hosts.has_key?(host_job_key)
  end

  # Enqueues the generated synthetic events to the state updater
  def enqueue_events(state_updater, events)
    state_updater.add_synthetic_events(events)
  end

  # Takes an event in REQUESTED state and generates synthetic events in RUNNING status for as many
  # timestamps as needed for this task to be finished. These tasks are assigned to the provisioned
  # hosts of this cluster and their used/free memory is updated accordingly.
  # Returns the generated synthetics events and the tasks that was not possible to be assigned
  # to the current resources.
  def delegate_event(event, host_job_key)
    granularity =  Constants::Numeric::GRANULARITY
    waiting_tasks = event.task_count # tasks in waiting status that need assignment

    # OPTIMIZE(stratos): Get the current ratio of the queue on the cluster
    # ie calculate the ratio of the number of tasks running for this job to the total number
    # of tasks on the cluster and then assign the same ratio on the provisioned resources

    task_assignments, waiting_tasks = calculate_tasks_per_host(@cluster.provisioned_hosts,
    waiting_tasks,
    @avg_task_mem)

    # Estimating how much time each task will run on host
    task_duration = (@avg_task_time.to_f/1000/60).floor

    events, @augmented_jobs_on_hosts, @gener_events_map = generate_events(task_assignments,
    task_duration,
    event.timestamp,
    granularity,
    host_job_key)

    return events, waiting_tasks
  end

  # Since the counter of the synthetic events in running status for a particular job and host
  # can change when new un-served REQUESTED events appear on the input as input_log_events
  # the synthetic events do not contain a valid task_count and this method returns the valid
  # task_count for these synthetic evens just before they are processed.
  def calc_run_event_count(event)
    if (@gener_events_map[event] == 0)
      fail InconsistentStateError, "Running count on event can't be 0"
    end
    @gener_events_map[event]
  end

  # Called only for original input_log_events with tasks that are on REQUESTED status but refer
  # to jobs that are already augmented. In this case the task_count that the input_log_event
  # contains is no longer valid and so this method returns the actual task_count for this
  # REQUESTED status based on the number of tasks that previously assigned to provisioned hosts
  # Gets as input the host_job_key of a task in REQUESTED status and the corresponding task_count
  # from the original input_log_event and returns the actual task_count
  def calc_req_event_count(host_job_key, task_count)
    augmented_tasks = @augmented_jobs_on_hosts[host_job_key]

    @logger.debug "calc_req_event_count called for:"
    host_job_key.print if @logger.debug?
    @logger.debug "task_count = #{task_count} - augmented_tasks = #{augmented_tasks}"
    remaining_tasks = task_count - augmented_tasks
    @logger.debug "remaining_tasks = #{remaining_tasks}"

    # OPTIMIZE: Negative task count its a good indication of REQUESTED tasks that started running
    # on non-provisioned hosts. That might because the avg_task_time is greater than the actual 
    # one for this particular task. Reducing this value from the running counters on the
    # non-provisioned hosts would probably make the simulation more accurate. This way we run
    # some of the requested tasks more than once!
    unless remaining_tasks >=0
      @logger.debug "Task count cannot be less than 0!"
      @logger.debug "remaining_tasks:#{remaining_tasks},\
                      task_count: #{task_count},\
                      augmented_tasks: #{augmented_tasks} "
      @logger.debug "Setting task count to 0!"
      remaining_tasks = 0
    end

    remaining_tasks
  end

  private

  def calculate_tasks_per_host(hosts, waiting_tasks, avg_task_mem)
    # Calculate assigned tasks per provisioned host
    no_fit = false  # true If none of the provisioned host has sufficient space
    task_assignments = Hash.new(0) # <host, assigned_containers>
    hosts_memory = {} # <host, free_memory>

    hosts.each_value do |host|
      hosts_memory[host.name] = host.free_memory
    end

    until (waiting_tasks == 0 || no_fit == true)
      no_fit = true
      hosts.each_value do |host|
        if (waiting_tasks == 0) then break end

        # Calculating how many tasks will be assigned to the host
        available_containers = (hosts_memory[host.name]  / avg_task_mem).floor

        @logger.debug "Avail_Containers:#{host.name}: #{available_containers}"
        (available_containers == 0)? next : no_fit = false

        # Assign only one container each time to distribute equally across available provisioned
        # hosts. This goes closer to how yarn works compared to filling up each host before  moving
        # to the next one with a command like this: [available_containers, waiting_tasks].min
        assigned_containers = 1
        waiting_tasks -= assigned_containers
        task_assignments[host] += assigned_containers

        hosts_memory[host.name]  -= avg_task_mem
      end
    end

    return  task_assignments, waiting_tasks
  end

  def generate_events(task_assignments, task_duration, event_timestamp, granularity, host_job_key)

    job_id = host_job_key.job_id
    events = []
    # Generate events for each host with assigned tasks
    task_assignments.each do |host, assigned_containers|
      # TODO: Update cluster memory for more fine grained calculation of cluster share
      # substract_cluster_memory(@cluster, assigned_containers, avg_task_mem)

      # Generate an event for each timestamp
      (0..task_duration).each  do |minute|
        gen_timestamp = event_timestamp + (minute * granularity)
        run_event = generate_run_event(gen_timestamp, @cluster.name, job_id, host.name)
        # If events exists update its assigned_containers count
        if (@gener_events_map.has_key?(run_event))
          @gener_events_map[run_event] += assigned_containers
          # If it is a new event assign the count for the first time and also return it part
          # of the new synthetic events
        else
          @gener_events_map[run_event] = assigned_containers
          events << run_event
        end

        # Keep track of the number of tasks augmented for this host_job_key
        if (@augmented_jobs_on_hosts.has_key?(host_job_key))
          @augmented_jobs_on_hosts[host_job_key] += assigned_containers
        else
          @augmented_jobs_on_hosts[host_job_key] = assigned_containers
        end
      end
    end

    return events, @augmented_jobs_on_hosts, @gener_events_map
  end

  # Creates running events but gives them nil as the task count. This is because we don't know
  # a-priori how many containers will be assigned for this <timestamp, cluster, job, host>
  # So we insert the run_event once in the priority queue and update its assigned_containers
  # field accordingly each time we have more running events for this timestamp. Updating the
  # key inside the queue without a separate map would be more time-costly.
  def generate_run_event(timestamp, cluster_name, job_id, host_name, assigned_containers = -1)
    task_status = Constants::TaskStatus::RUNNING
    synthetic_event = SyntheticEvent.new(timestamp, cluster_name, job_id, host_name\
    , task_status, assigned_containers)

    return synthetic_event
  end

  def subtract_host_memory(host, assigned_containers, avg_task_mem)
    memory_used = assigned_containers * avg_task_mem
    host.memory_used += memory_used
    if host.free_memory < 0
      fail InconsistentStateError, "Host memory cannot be negative!"
    end
  end

  def substract_cluster_memory(cluster, assigned_containers, avg_task_mem)
    # Method not currently used
    memory_used = assigned_containers * avg_task_mem
    cluster.memory_used += memory_used
    if cluster.free_memory < 0
      fail InconsistentStateError, "Cluster memory cannot be negative!"
    end
  end

  def get_cluster_capacity(cluster)
    # Returns the total amount of memory the cluster has
  end

  def get_max_host_mem_available(host)
    # Returns the free memory a host has

  end

  def get_max_host_mem_capacity(host)
    # Returns the total memory capacity of a host

  end
end