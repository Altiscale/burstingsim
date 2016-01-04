require 'test/unit'

require_relative '../events_stream'
require_relative '../events/provision_event'

class EventsStreamTest < Test::Unit::TestCase
  def setup
    @stream = EventsStream.new
    @events = []
    @events[0] = InputLogEvent.new('1430438400', 'test_cluster', 'test_job_id', 'test_host', 'RUNNING', '10')
    @events[1] = InputLogEvent.new('1430438460', 'test_cluster', 'test_job_id', 'test_host', 'RUNNING', '10')
    @events[2] = InputLogEvent.new('1430438520', 'test_cluster', 'test_job_id', 'test_host', 'RUNNING', '10')

    @sched_events = []
    @sched_events[0] = ProvisionEvent.new('1430438460', 'test_colloc', 'test_cluster', 'test_host')
    @sched_events[1] = ProvisionEvent.new('1430438460', 'test_colloc', 'test_cluster', 'test_host')
  end

  def test_push_one_event
    @events.each do |e|
      @stream.push(e)
    end

    popped = @stream.pop.timestamp
    expected = 1430438400
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

    popped = @stream.pop.timestamp
    expected = 1430438460
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

    popped = @stream.pop.timestamp
    expected = 1430438520
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

    popped = @stream.pop
    expected = nil
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

  end

  def test_multiple_events
    @stream.concat(@events)

    popped = @stream.pop.timestamp
    expected = 1430438400
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

    popped = @stream.pop.timestamp
    expected = 1430438460
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

    popped = @stream.pop.timestamp
    expected = 1430438520
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

    popped = @stream.pop
    expected = nil
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")
  end

  def test_scheduling_events
    @stream.concat(@sched_events)

    popped = @stream.pop.timestamp
    expected = 1430438460
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

    popped = @stream.pop.timestamp
    expected = 1430438460
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

    popped = @stream.pop
    expected = nil
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")
  end

  def test_mixed_events
    @stream.concat(@events)
    @stream.concat(@sched_events)

    popped = @stream.pop
    expected = 1430438400
    assert(popped.timestamp == expected, "Expected: #{expected} - Found: #{popped}")
    assert(popped.is_a?(InputLogEvent), "Expected: InputLogEvent - Found: #{popped.class}")

    popped = @stream.pop
    expected = 1430438460
    assert(popped.timestamp == expected, "Expected: #{expected} - Found: #{popped}")
    assert(popped.is_a?(ProvisionEvent), "Expected: ProvisionEvent - Found: #{popped.class}")

    popped = @stream.pop
    expected = 1430438460
    assert(popped.timestamp == expected, "Expected: #{expected} - Found: #{popped}")
    assert(popped.is_a?(ProvisionEvent), "Expected: ProvisionEvent - Found: #{popped.class}")

    popped = @stream.pop
    expected = 1430438460
    assert(popped.timestamp == expected, "Expected: #{expected} - Found: #{popped}")
    assert(popped.is_a?(InputLogEvent), "Expected: InputLogEvent - Found: #{popped.class}")

    popped = @stream.pop
    expected = 1430438520
    assert(popped.timestamp == expected, "Expected: #{expected} - Found: #{popped}")
    assert(popped.is_a?(InputLogEvent), "Expected: InputLogEvent - Found: #{popped.class}")

    popped = @stream.pop
    expected = nil
    assert(popped == expected, "Expected: #{expected} - Found: #{popped}")

  end
end
