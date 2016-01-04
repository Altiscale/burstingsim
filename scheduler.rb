require 'observer'
require_relative 'lru'
require_relative 'pqueue'
require_relative 'moving_window'
require_relative 'modules/constants'
require_relative 'policies/replay_policy'
require_relative 'policies/fair_share_policy'
require_relative 'policies/simple_policy'
require_relative 'events/provision_event'
require_relative 'events/eviction_event'
require_relative 'state_updater'

# Scheduler class is an observer to the state of the world. Each time the timestamp of the state changes
# the scheduler is triggered to update the demand and offer models and create  provision and eviction events according
# to a scheduling policy. Demand is calculated on a per cluster basis while offer is calculated on a per host basis. 
# The scheduler will call a scheduling policy giving it the current offer and demand models and the policy will 
# generate the provision and eviction plans that the scheduler will convert to provision and eviction scheduling
# events and it will send them to the world via the state updater.
class Scheduler 
  attr_accessor :policy,
  :demand_map,
  :offer_map,
  :window_slots
  
  def initialize(world_state, scheduler_config, window_slots=5)
    world_state.add_observer(self)
    
    @policy = case scheduler_config['policy']
      when Constants::SchedulingPolicies::REPLAY
        ReplayPolicy.new
      when Constants::SchedulingPolicies::FAIR_SHARE
        FairSharePolicy.new   
      when Constants::SchedulingPolicies::SIMPLE
        SimplePolicy.new 
      else raise PolicyNotSupportedError, "Policy #{scheduler_config['policy']} is not supported"
    end
    
    @window_slots = window_slots
    @events_granularity = Constants::Numeric::GRANULARITY
    @window_size = @window_slots * @events_granularity #The window time in seconds
    @scheduler_config = scheduler_config

    puts "Scheduler model window size (seconds - slots): #{@window_size} - #{@window_slots}"
    
    @demand_map = Hash.new {|hash, key| hash[key] = Hash.new} #<colloc, <cluster_name, demand>>
    @offer_map = Hash.new {|hash, key| hash[key] = Hash.new} #<colloc, <host_name, offer>>
    
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::INFO   
    
    @logger.info "Scheduler running with policy: #{@policy.name}"
    
  end
  
  def update(world_state) #Callback for observer
    return if (@policy.is_a?(ReplayPolicy))
    
    timestamp = world_state.timestamp
    world = world_state.world  
    
    world.collocs.each do |colloc_name, colloc| 
      
      update_offer_and_demand(colloc_name, 
                              colloc.full_clusters.clone, 
                              colloc.free_hosts.clone, 
                              colloc.inventory_hosts.clone,
                              colloc.busy_hosts.clone,
                              colloc.transitioning_hosts.clone)
      
      offer_schedulables, demand_schedulables = @policy.compute_schedulables(@offer_map[colloc_name],
                                                                             @demand_map[colloc_name])
      
                                                                          
                                                                                                                                                                               
      if (@scheduler_config['allow_provisions'])
        prov_plan = @policy.generate_provision_plan(demand_schedulables, colloc.inventory_hosts)
      end
      
      if (@scheduler_config['allow_evictions'])
        evict_plan = @policy.generate_eviction_plan(offer_schedulables, colloc.free_hosts)  
      end         
      
      unless prov_plan.nil? || prov_plan.empty? 
        @logger.debug "Time: #{timestamp} - Provision plan:"
        puts prov_plan.inspect if @logger.debug?
      end
      
      unless evict_plan.nil? || evict_plan.empty?
        @logger.debug "Time: #{timestamp} - Eviction plan:"
        puts evict_plan.inspect if @logger.debug?
      end
      
      prov_events, prov_hosts = generate_provision_events(timestamp, 
                                                          colloc_name,
                                                          prov_plan,
                                                          @scheduler_config['provision_delay'])
                                                            
      evict_events, evict_hosts = generate_eviction_events(timestamp,
                                                           colloc_name,
                                                           evict_plan,
                                                           @scheduler_config['eviction_delay'])
                                                         
      move_affected_hosts(timestamp, prov_hosts, evict_hosts, colloc)                                                 
      send_scheduling_events(prov_events, evict_events)                                                
      
   end  
  end

   
  def print_demand
    puts "DEMAND"
    @demand_map.each do |colloc_name, cluster_demand|
       puts "#{colloc_name}:"
       cluster_demand.each do |c, d| 
         puts "#{c} (window - consec. - tasks): #{d.window_count} - #{d.consec_count} - #{d.waiting_tasks}"
       end
     end
     #sleep 2
  end
  
  def print_offer
    puts "OFFER"
    @offer_map.each do |colloc_name, host_offer|
        puts "#{colloc_name}:"
        host_offer.each { |h, o| puts "#{h}: #{o.status}"}
      end
  end
  
  private
  
  # Sends signal to colloc to mark nodes as moving
  def move_affected_hosts(timestamp, prov_hosts, evict_hosts, colloc) 
    colloc.moving_from_inventory(timestamp, prov_hosts) 
    colloc.moving_to_inventory(timestamp, evict_hosts)
  end
  
  def send_scheduling_events(prov_events, evict_events)
    return if prov_events.nil? && evict_events.nil? 
    
    StateUpdater.add_sched_events(prov_events)
    StateUpdater.add_sched_events(evict_events)
  end
  
  def generate_provision_events(timestamp, colloc_name, prov_plan, prov_delay)
    return if prov_plan.nil? || prov_plan.empty?
    
    @logger.debug "#{timestamp}: Generating provision events!"
    events = []
    hosts = []
    prov_plan.each do |host_name, cluster_name|
        provision_time = [timestamp, timestamp + (prov_delay * @events_granularity)].max
        hosts << host_name
        events << ProvisionEvent.new(provision_time, colloc_name, cluster_name, host_name)
    end
      
    return events, hosts
  end
  
  def generate_eviction_events(timestamp, colloc_name, evict_plan, evict_delay)
    return if evict_plan.nil? || evict_plan.empty?
    
    events = []
    hosts = []
    evict_plan.each do |host_name, cluster_name|
        eviction_time = [timestamp, timestamp + (evict_delay * @events_granularity)].max
        hosts << host_name
        events << EvictionEvent.new(eviction_time, colloc_name, cluster_name, host_name)
    end
    
    return events, hosts      
  end
  
  def print_schedulables(schedulables, type)
    return unless (!schedulables.nil?  && !schedulables.empty?)
    
    puts "#{type} schedulables:"
    schedulables.to_a.each { |s| puts "#{s.name} - #{s.score}"}
  end
  
  def update_offer_and_demand(colloc_name, full_clusters, free_hosts, inventory_hosts, busy_hosts, transitioning_hosts)
    update_demand(colloc_name, full_clusters)
    update_offer(colloc_name, free_hosts, inventory_hosts, busy_hosts, transitioning_hosts)
  end
  
  def update_demand(colloc_name, full_clusters)
    demand_map = @demand_map[colloc_name]
    if (demand_map.empty? || demand_map.nil?)
      return register_new_provision_candidates(colloc_name, full_clusters)
    end

       demand_map.each do |cluster_name, demand|
         if (!full_clusters.nil? && !full_clusters.empty? && full_clusters.has_key?(cluster_name))
           full = true
           consec_count = demand.consec_count + 1
           waiting_tasks = full_clusters[cluster_name].waiting_tasks_count
         else
           full = false
           consec_count = 0        
           waiting_tasks = 0 #Since its not full waiting tasks should be 0 
         end
       
       demand_map[cluster_name].update(full, consec_count, waiting_tasks)   
       full_clusters.delete(cluster_name) #Cluster is processed
    end
    
    #Register whatever wasn't existing
    register_new_provision_candidates(colloc_name, full_clusters)
  end
  
  def register_new_provision_candidates(colloc_name, clusters)
    return unless !clusters.empty? #Add newly appeared clusters to schedulables
    clusters.each do |cluster_name, cluster|
      @demand_map[colloc_name][cluster_name] = Demand.new(cluster_name, @window_slots, cluster.waiting_tasks_count)
    end
  end
  
  # *Development of method in progress*
  def update_offer(colloc_name, free_hosts, inventory_hosts, busy_hosts, transitioning_hosts)
    # TODO(stratos): If offer map keeps references to hosts then we don't need to also have the inventory hosts
    # as parameter but we can just check the status of the host we process from the map
    offer_map = @offer_map[colloc_name]
    if (offer_map.empty? || offer_map.nil?) 
      return register_new_eviction_candidates(colloc_name, free_hosts)
    end
     
    offer_map.each_key do |host_name|
      if(inventory_hosts.has_key?(host_name))
        host = inventory_hosts[host_name]
        @offer_map[colloc_name].delete(host_name) #Node back to inventory so delete stats from offer map
        inventory_hosts.delete(host_name)
        next
      elsif(free_hosts.has_key?(host_name))
        host = free_hosts[host_name]
        free_hosts.delete(host_name)
       elsif(transitioning_hosts.has_key?(host_name))
        host = transitioning_hosts[host_name]
        transitioning_hosts.delete(host_name)
       elsif (busy_hosts.has_key?(host_name))
        host = busy_hosts[host_name]
        busy_hosts.delete(host_name)
        else
          
        # TODO(stratos): Implement logic - Host not free or in inventory anymore - Update stats
        # TODO(stratos): How about the stats of a host that changed clusters? If there is cluster-related info 
        # on how the offer is calculated this should be considered as well.
      end
      
      provisioned = host.provisioned
      status = host.status
      
      offer_map[host_name].update(status, provisioned)
    end
      
    register_new_eviction_candidates(colloc_name, free_hosts)
    
  end
 
  def register_new_eviction_candidates(colloc_name, free_hosts)
    return unless !free_hosts.nil? && !free_hosts.empty?
    
    free_hosts.each do |host_name, host|
       @offer_map[colloc_name][host_name] = Offer.new(host_name, 
                                                      host.cluster_name,
                                                      @demand_map[colloc_name][host.cluster_name],
                                                      host.status, 
                                                      host.provisioned)
     end
       
  end
  
  # *Development in progress* Represents the offer model for each host.
  class Offer
    # TODO(stratos): Implement a similar to Demand notion of Offer based on how frequent a host is free, 
    # if it is an inventory node etc, Offer should be per host and for example the counter should 
    # be reseted each time the host goes back to inventory
    attr_reader :host_name,
    :cluster_name,
    :cluster_demand,
    :status, #free/ busy/ inventory/ moving etc
    :provisioned
    
    def initialize(host_name, cluster_name, cluster_demand, host_status, provisioned)
      @host_name = host_name
      @cluster_name = cluster_name
      # The offer model of each host also contains the demand of it's cluster
      @cluster_demand = cluster_demand
      @status = host_status
      @provisioned = provisioned
      unless (@status ==  Constants::HostStatus::FREE)
        raise InconsistentStateError, "Offer created but host not free! (host #{@host_name} status: #{@status})"
      end
    end  
    
    def update(status, provisioned)
      @status = status
      @provisioned = provisioned
    end
  end
    
  # Represents the demand model for each cluster. Currently the demand components are how many times the cluster
  # was in "full" capacity for a given window, for a number of consecutive slots and how many tasks are in
  # waiting status
  class Demand
    # OPTIMIZE(stratos): I keep waiting_tasks as part of the demand to increase flexibility of future implemented policies. 
    # If not used though, a more space-efficient approach would be to keep the task_count on a separate Hash than 
    # full count, as currently I am using only the last updated task count
    attr_reader :cluster_name,
       :window_count,
       :consec_count, 
       :waiting_tasks
       
       def initialize(cluster_name, window_slots, waiting_tasks)
         @cluster_name = cluster_name
         @window = MovingWindow.new(window_slots)
         @window.write(1)
         @window_count = @window.sum()
         @waiting_tasks = waiting_tasks
         @consec_count = 1
       end
   
       def update(full, consec_count, waiting_tasks)
         (full)? @window.write(1) : @window.write(0)
         @window_count = @window.sum()
         @consec_count = consec_count
         @waiting_tasks = waiting_tasks
       end
       
       # TODO(stratos): Modify comperator to compare demands directly?
       def eql?(another_key)
         @window_count == another_key.window_count && @consec_count == another_key.consec_count \
           && @waiting_tasks == another_key.waiting_tasks
       end
   
       def hash
         [@window_count, @consec_count, @waiting_tasks].hash
       end
  end
  
end
