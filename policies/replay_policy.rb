require_relative 'scheduling_policy'

class ReplayPolicy < SchedulingPolicy
    
   attr_reader :name
       
   def initialize
     @name = Constants::SchedulingPolicies::SIMPLE
     @logger = Logger.new(STDOUT)
     @logger.level = Logger::INFO  
   end
       
end
