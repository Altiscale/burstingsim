require_relative 'event'

# Events of this type cause updates on the entities of the world model. 
# From this class derive the input_log_events and the synthetic events
class UpdateEvent < Event
  
  attr_accessor :cluster_name, #ex: marketshare,
                :job_id, #ex: job_1418689719855_8369hostName,
                :host_name, #ex: 203-01-03.sc1.verticloud.com
                :colloc_name, #the sc1 or ac1 part of the hostName
                :task_status, #RUNNING, REQUESTED etc
                :task_count #The number of tasks on this status
     
def initialize (timestamp, cluster_name, job_id, host_name, task_status, task_count)
  @cluster_name = cluster_name
  @job_id = job_id
  @host_name = host_name
  @task_status = task_status
  @task_count = task_count.to_i
  
    super(timestamp)
end     

# Events are aggregates for a particular task_status - So there shouldn't be more than one
# events for the same timestamp, cluster, job, host and status 
def eql?(another_key)
 @timestamp == another_key.timestamp && 
 @cluster_name == another_key.cluster_name && 
 @job_id == another_key.job_id && 
 @host_name == another_key.host_name &&
 @task_status == another_key.task_status
end
 
def hash
 [@timestamp, @cluster_name, @job_id, @host_name, @task_status].hash
end

def print 
  puts "#{timestamp},#{cluster_name},#{job_id},#{host_name},#{task_status},#{task_count}"
  
end
end   