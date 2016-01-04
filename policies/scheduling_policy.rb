require 'logger'
require_relative '../schedulable'

class SchedulingPolicy
  attr_reader :name
       
  def initialize
   @name = Constants::SchedulingPolicies::SIMPLE
   @logger = Logger.new(STDOUT)
   @logger.level = Logger::INFO  
  end
  
  def generate_provision_plan(demand_schedulables, available_resources)
  end
  
  def generate_eviction_plan(offer_schedulables)
  end

  # TODO: Following methods would be useful for some policies (ex: fair-share). *Implementation in progress*
  def compute_schedulables(offer_map, demand_map)
  end
  
  def usage_over_fair_share?
  end

  def usage_over_max_share?
  end

  def usage_bellow_min_share?
  end
end
