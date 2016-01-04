require_relative 'update_event'

# Events coming from the input. They are treated similar to all UpdateEvents.
# Separate class created just because we need to distinguish at runtime between the different type
# of events that cause updates to the model
class InputLogEvent < UpdateEvent

end   