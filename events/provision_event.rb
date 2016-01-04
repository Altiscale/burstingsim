require_relative 'scheduling_event'
# An event that specifies getting a node from inventory and provisioning to a cluster
# Current used only to differentiate from an EvictionEvent so it can be treated different at runtime
class ProvisionEvent < SchedulingEvent
  def initialize(timestamp, colloc_name, cluster_name, host_name)
    super(timestamp, colloc_name, cluster_name, host_name)
  end
end