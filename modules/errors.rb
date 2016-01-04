# Contains the different type of errors that can occur at runtime.
class ParseError < StandardError
end

class InconsistentStateError < StandardError
end

class MonotonicTimeIncreaseError < StandardError
end

class UnknownInputEventStatusError < StandardError
end

class NoEventsInQueueError < StandardError
end

class HostNotInCluster < StandardError
end

class PolicyNotSupportedError < StandardError
end

class UnsupportedHostStatusError < StandardError
end

class UnknownSchedulingEventTypeError < StandardError
end

class MovingBusyHostError < StandardError
end

class UnknownEventTypeError < StandardError
end

