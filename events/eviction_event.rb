require_relative 'scheduling_event'
# An event that specifies de-registering a node from a cluster and taking it back to inventory
# Currently used only to differentiate from an ProvisionEvent so it can be treated different at runtime
class EvictionEvent < SchedulingEvent
  def initialize(timestamp, colloc_name, cluster_name, host_name)
    super(timestamp, colloc_name, cluster_name, host_name)
   end
end