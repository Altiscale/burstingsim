require_relative 'event'
# Events created by scheduling decisions. Both provision and eviction events should 
# inherit from this class.
class SchedulingEvent < Event
  attr_accessor :colloc_name, # the sc1 or ac1 part of the hostName
                :cluster_name, # ex: marketshare,
                :host_name # ex: 203-01-03.sc1.verticloud.com

  def initialize(timestamp, colloc_name, cluster_name, host_name)
    @colloc_name = colloc_name
    @cluster_name = cluster_name
    @host_name = host_name

    super(timestamp)
  end

  def print
    puts "#{timestamp},#{colloc_name},#{cluster_name},#{host_name}"
  end
end