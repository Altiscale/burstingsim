# Trace is the recording of a transformation of a state to another state that is the outcome of applying one event
# Each simulation round produces one Trace
class Trace
  attr_accessor :before_state,
                :event,
                :after_state

  def initialize(before_state, event, after_state)
    @before_state = before_state
    @event = event
    @after_state = after_state
  end
end
