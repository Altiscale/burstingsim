require_relative 'event'

# Events created to control simulation, such as an event indicating the simulation end.
class ControlEvent < Event
  
  attr_accessor :message
      
def initialize (timestamp, message)
  @message = message 
  super(timestamp)
end     

def print 
  puts "#{timestamp},#{message}"
  
end
end   