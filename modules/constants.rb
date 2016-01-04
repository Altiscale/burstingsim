module Constants
  class TaskStatus
    REQUESTED = 'REQUESTED'
    RESERVED = 'RESERVED'
    ALLOCATED = 'ALLOCATED'
    ACQUIRED = 'ACQUIRED'
    RUNNING = 'RUNNING'
    EXPIRED = 'EXPIRED'
    # Done is a derived status. Doesn't exist on the input. 
    # A task goes from any of its other statuses to a DONE status whenever is
    # detected that the Job or Host that the task was referring to is 
    # finished.
    DONE = 'DONE' 
  end

  class JobStatus
    # Actual possible YARN job states are: 
    # ALL, NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, \
    # FAILED, KILLED 
	  # but we don't have this info on input
    UNDEFINED = 'UNDEFINED'
    WAITING = 'WAITING'
    RUNNING = 'RUNNING'
    FINISHED = 'FINISHED'
  end

  class HostStatus
    UNDEFINED = 'UNDEFINED'
    INVENTORY = 'INVENTORY' # Host on the inventory
    # Hosts that no longer appear on hosts without knowing if they went back to inventory
    OFFLINE = 'OFFLINE'
    BUSY = 'BUSY'
    FREE = 'FREE'
    REQUESTED = 'REQUESTED'
    RESERVED = 'RESERVED'
    ALLOCATED = 'ALLOCATED'
    MOVING = 'MOVING'# Indicates moving from/to inventory by the orchestrator
  end

  class TaskStatusCountersOwners
    JOB = 'JOB'
    HOST = 'HOST'
    CLUSTER = 'CLUSTER'
  end

  class ControlMessages
    SIM_IS_OVER = 'Simulation_is_Over'
    GAP = 'Filling_gap_on_input'
  end

  class Numeric
    GRANULARITY = 60 # Seconds difference between timestamps
  end

  class SchedulingPolicies
    REPLAY = 'REPLAY'
    FAIR_SHARE = 'FAIR_SHARE'
    SIMPLE = 'SIMPLE'
  end
  
  # TODO: All these should be replaced with host/job specific derived from the memory_per_host.csv
  # and run_times_per_task.csv
  class ResourceValues
    AVG_TASK_TIME = 25020
    AVG_TASK_MEM = 1536
    MEMORY_CAPACITY = 45056
  end

end