require_relative 'world'
require 'observer'

# The state of the world at each given timestamp.
class WorldState
  include Observable

  attr_accessor :timestamp,
                :world
  def initialize(world_config)
    @timestamp = 0
    @world = World.new(world_config)
  end

  def print_summary
    @world.print_summary
  end

  def print_all
    @world.print_all
  end

  def update(state_updater, event)
    timestamp = event.timestamp
    if (@timestamp != timestamp)
      world.timestamp_changed_signal(timestamp)
      changed # Notify observers like the scheduler
      notify_observers(self)
      @timestamp = timestamp
    end
    world.update(state_updater, event) # Send 1st event of the new timestamp
  end
end
