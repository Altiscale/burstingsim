#Every event should have a timestamp. All event classes should derive from this class.
class Event
  include Comparable
  
  attr_accessor :timestamp #ex:1420218660
  
  def <=> (another_event)
    @timestamp <=> another_event.timestamp
  end
  
  def initialize(timestamp)
    @timestamp = timestamp.to_i
  end
 
end