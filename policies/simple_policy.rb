require_relative 'scheduling_policy'

# Initial implementation of a scheduling policy. Orders clusters based on the number of tasks they have in waiting
# status and assigns to them a free host from the inventory.
# At the moment 
class SimplePolicy < SchedulingPolicy
     
    # Computes two priority queues of clusters based on their level of demand and hosts based on their level of freedom
    def compute_schedulables(offer_map, demand_map)       
        
      offer_schedulables = compute_offer_schedulables(offer_map)  
      demand_schedulables = compute_demand_schedulables(demand_map)
      
      return offer_schedulables, demand_schedulables
      
    end
    
    def generate_provision_plan(demand_schedulables, available_resources)
      # Maps demand schedulables to the available resources
      return unless exist?(demand_schedulables) && exist?(available_resources)
      
      provision_plan = Hash.new #<host_name, cluster_name>
      
      available_resources.each_key do |host_name|
        (demand_schedulables.empty?)? break : provision_plan[host_name] = demand_schedulables.pop.name
      end
      
      return provision_plan
    end
    
    # Selects the hosts to be evicted 
    def generate_eviction_plan(offer_schedulables, free_hosts)
      return unless exist?(offer_schedulables)    
      eviction_plan = Hash.new  #<host_name, cluster_name>   
      
      until offer_schedulables.empty?
        evicted_host = offer_schedulables.pop.name
        eviction_plan[evicted_host] = free_hosts[evicted_host].cluster_name
      end
      
      return eviction_plan
    end
    
    private
    
    def exist?(schedulables)
      (schedulables.nil? || schedulables.empty?)? false : true
    end
    
    def compute_offer_schedulables(offer_map)
        offer_schedulables = PQueue.new #All the scheduled events for this round ordered by priority
        return unless !offer_map.empty? && !offer_map.nil?
        
        offer_map.each do |host_name, offer|
          # Select only free hosts that are provisioned and their cluster doesn't have waiting tasks
          if (offer.status == Constants::HostStatus::FREE && 
              offer.provisioned &&
              offer.cluster_demand.waiting_tasks == 0)
            score = 1
            offer_schedulables.enq(Schedulable.new(host_name, score))
          end
        end
        offer_schedulables
      end
    
    # Orders clusters with demand based on the number of tasks they have in waiting status  
    def compute_demand_schedulables(demand_map)
      demand_schedulables = PQueue.new #All the scheduled events for this round ordered by priority
      return if demand_map.nil? || demand_map.empty?
      
      # Ordering based on how many tasks in waiting status
      demand_map.each do |cluster_name, demand|
        if (demand.waiting_tasks > 0)
          score = demand.waiting_tasks
          demand_schedulables.enq(Schedulable.new(cluster_name, score))
        end
      end
      demand_schedulables
    end
   
    def compute_shares(schedulables, total_resources)
    end
    
    def usage_over_fair_share?()
    end
    
    def usage_over_max_share?()
    end
    
    def usage_bellow_min_share?()
    end
end