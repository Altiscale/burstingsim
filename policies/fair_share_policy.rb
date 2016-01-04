require_relative 'scheduling_policy'

#TODO: *In progress*
class FairSharePolicy < SchedulingPolicy
    
    def apply(offer_map, demand_map, resources)
      @schedulables = PQueue.new #All the scheduled events for this round ordered by priority
      
      if (offer_map.nil? || offer_map.empty?)
        return @schedulables
      end
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
