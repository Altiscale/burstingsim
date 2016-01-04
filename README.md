## General Comments
### Run code
- Add 'abstraction' ruby gem to your environment
- To run the tests go to the test directory and run ruby test-suit.rb.
- To run the simulator:
  - You need to download from s3 the input file: s3://com.altiscale.mt.perf/input-1m.csv
  - Then just run ruby simulation.rb

### Code flow - Summary of main classes:
- The simulation.rb starts the simulation. Parser.rb will parse the file and insert all rows as events on the stream_generator that is based on a priority queue. 
Then the state_updater is responsible to get the events from the stream_generator and send them to the world. The world class contains all the classes involved (clusters, collocs, hosts, jobs etc). 
The event will be processed by all affected objects.
  - parser/ stream-generator: The file is loaded into memory and each of its lines are converted to input_log_events and inserted into a priority queue
  - simulation: The simulator will first take a training window of events an process them through the state_updater to build an initial world state. Then the remaining events from the stream generator will be send to the state updater.
  - state_updater: Gets the events from the stream generator and applies them one by one to the world creating each time a new world state
  - world: Is the class that implements the model of the simulator. Contains colloc, clusters, hosts and jobs classes that are updated with each incoming event deriving each time a new state. Read the documentation bellow for more details. 

### Sources of events:
  - **InputLogEvents:** Contain all the events from the dogfood database letting us know about changes on *running* clusters, hosts, jobs, etc. “Running” is emphasized because we are not going to get events  letting us know that a task or a job is finished or that a host exists but not used by a cluster and so on. A new key for a the key <cluster, host, job, task_status> is coming every minute.
Example input:<br />
*1432940760, motleyfool, job_1431378945963_1330, 202-17-03.sc1.verticloud.com, ALLOCATED, 2*<br />
*1432940760, motleyfool, job_1431378945963_1330, 202-17-03.sc1.verticloud.com, ACQUIRED, 2*<br />
*1432940820, motleyfool, job_1431378945963_1330, 202-17-03.sc1.verticloud.com, RUNNING, 2*<br />
  - **DerivedEvents:** On the absence of InputLogEvents for more than one minute for:
    1. a job on a host (key: [[host,job]]), 
    2. job on a cluster (key: [[job, cluster]]) and 
    3. task status of a job on a particular host (key: [[host, job, task_status]])
    
 a derived event is created that triggers  signals on all affected entities in order to update their state accordingly (note: There is no actual event object created, even though it could be for better flow clarity with an additional performance cost)

## World
- **Registration:**
  - Provides methods for the registration of collocs, clusters and hosts
- **Access:**
  - Has access to collocs, hosts, and clusters
- **Status Updates:**
  - No status assigned to world
- **On update:**
  - Will get all the affected resources (cluster, hosts, collocs and jobs) from the incoming event
  - Update the entries of affected resources on the LRU cache
  - Calculates the task count difference for the task_status, host_name and job_id that the incoming event refers to. This difference will be send on the update methods of job, host and cluster as part of their update method
  - Calculates the finished jobs that are finished on hosts and clusters, updates the counters it keeps and sends the relevant signals to their clusters
  - Tracks hosts that changed clusters
  - Calls the update method of job, host and cluster so they can process the event
- **On Job Finish:**
  - World is the starting point from which finished jobs are calculated and the corresponding signals are send to the clusters that the jobs belong to.
- **Interactions:**
  - Entities: state_updater, colloc, cluster, host
  - IN:
    - (external) update, print << state_updater
    - register_x with x = colloc, cluster, host
  - OUT:
    - job_finished_signal >> cluster
    - job_done_on_host_signal >> cluster
    - task_status_update_signal >> cluster

## Colloc
*  Colloc is a convenience abstraction just to keep the free/busy hosts so the scheduler can select a free host from the same colloc that the cluster with additional needs belongs to
- **Registration:**
  - With world on construction
- **Access:**
  - hosts
- **On update:**
  - Colloc doesn’t implement its own update method. Busy/ Free hosts are updated through signalling from the host
- **Interactions:**
    - IN: host_status_changed_signal << Host

## Cluster
- **Registration:**
  - With world on construction
- **Access:**
  - hosts and jobs
- **Status Updates:**
  - No status assigned to cluster
- **On update:**
  - Update task status counters
  - checks for inconsistencies
- **On Job Finish:**
  - Gets separate signals from world for job done on host (job_is_done_on_host_signal) and job finished (job_is_finished_signal)
- **Interactions:**
  - Entities: world, job, host
  - IN: 
    - job_is_finished_signal << world
    - job_is_done_on_host_signal << world
    - job_status_changed_signal << job
    - host_status_changed_signal << host
  - OUT:
    - job_is_done_on_host_signal >> job and host

## Host
- **Registration:**
  - With world on construction
- **Access:**
  - Cluster
  - task status counters
- **Status Updates:**
  - On registration/deregistration
- **On update:**
  - Checks if it changed cluster
  - Registers a jobs that appears for the first time
  - Updates task status counters
  - Updates status
  - Each time status is changed signal is send to cluster
- **On Job Finish:**
  - When job is done on particular host
    - De-registers job
    - updates status
  - Resets counters if no active jobs
- **Interactions:**
  - Entities: cluster
  - IN: job is done on host << cluster
  - OUT: status changed >> cluster
Job
- **Registration:**
With cluster on creation
- **Access:**
  - Nothing - cluster object is passed on updated
- **Status Updates:**
  - On update: based on the “active” task_status_counters
- **On update:**
  - add host to used hosts set
  - update task status counters
  - update status
  - send job_state_change signal to cluster
- **On Job Finish:**
  - done_on_cluster
    - Sets status to FINISHED
    - Resets task counters and used hosts set
  - done_on_host
    - Host is removed from used hosts
- **Interactions:**
  - Entities: cluster
  - IN: 
    - done_on_cluster_signal << cluster
    - done_on_host_signal << cluster
  - OUT: job_status_changed_signal >> cluster
