require 'logger'

require_relative 'modules/errors'
require_relative 'world_state'
require_relative 'events_stream'

# Gets events and applies them to the current world state
class StateUpdater

  attr_accessor :input_queue, # PQueue of input_log_events and control_events
  :sched_queue, # PQueue of scheduling events.
  :synth_queue # PQueue of synthetic events

  @logger = Logger.new(STDOUT)
  @logger.level = Logger::INFO
  @input_queue = EventsStream.new
  @sched_queue = EventsStream.new
  @synth_queue = EventsStream.new # Events synthetic for the provisioned hosts
  # Works as a reactor multiplexing the events coming from the different queues and selecting
  # the one with higher order (most recent timestamp) to dispatch and update the world state
  @queues_multiplexer = PQueue.new([]) { |a, b| b <=> a }

  def self.apply_event(state, event)
    if event.timestamp < state.timestamp
      puts 'Event caused exception:'
      event.print
      fail MonotonicTimeIncreaseError, 'Timestamps not monotonically increasing!'
    end

    @logger.debug "StateUpdater sending..."
    event.print if @logger.debug?

    state.update(self, event)

    state
  end

  def self.process_stream(state, window=-1)
    # Window is in days. When it is -1 then process all events of the stream
    return if state.nil?

    start_date = @input_queue.top.timestamp
    window_in_minutes = window * 60 * 60 * 24

    end_date = (window == -1)? @input_queue.bottom.timestamp : start_date + window_in_minutes

    events_sent = 0

    #Initialize multiplexer
    fill_multiplexer(end_date)

    until (@queues_multiplexer.empty?)
      event = @queues_multiplexer.pop()

      if (event.is_a?(InputLogEvent) || event.is_a?(ControlEvent))
        @input_queue.pop
      elsif (event.is_a?(SchedulingEvent))
        @sched_queue.pop
      elsif (event.is_a?(SyntheticEvent))
        @synth_queue.pop
      else
        fail UnknownEventTypeError, "Event type not supported!"
      end

      state = apply_event(state, event)
      events_sent += 1

      # OPTIMIZE: Instead of clearing and re-pushing from all queues maybe i can replace the
      # elements only if the queue had new events
      @queues_multiplexer.clear unless @queues_multiplexer.empty?
      fill_multiplexer(end_date)
    end

    return state, events_sent
  end

  def self.top
    @queues_multiplexer.top
  end

  def self.set_stream(stream)
    @input_queue = stream
  end

  def self.stream
    @input_queue
  end

  def self.add_synthetic_events(events)
    #Adds events to the synthetic queue
    add_events(@synth_queue, events)
  end

  def self.add_sched_events(events)
    #Adds events to the scheduling queue
    add_events(@sched_queue, events)
  end

  def self.add_stream_events(events)
    #Adds events to the scheduling queue
    add_events(@input_queue, events)
  end

  def self.empty_events
    @queues_multiplexer.clear unless @queues_multiplexer.nil?
    @input_queue.clear unless @input_queue.nil?
    @sched_queue.clear unless @sched_queue.nil?
    @synth_queue.clear unless @synth_queue.nil?
  end

  def self.has_events?
    (@queues_multiplexer.empty?)? false : true
  end

  private

  def self.fill_multiplexer(end_date)
    # Push to the multiplexer each time the event with the highest priority
    @queues_multiplexer.push(@input_queue.top) if has_valid_events?(@input_queue, end_date)
    @queues_multiplexer.push(@sched_queue.top) if has_valid_events?(@sched_queue, end_date)
    @queues_multiplexer.push(@synth_queue.top) if has_valid_events?(@synth_queue, end_date)
  end

  # Adds events to the given queue
  def self.add_events(queue, events)
    return if events.nil?
    queue.concat(events)
  end

  def self.add_event(queue, event)
    return if event.nil?
    queue.push
  end

  def self.has_valid_events?(queue, date)
    processed_until_date?(queue, date)? false : true
  end

  # Checks if the given queue has already processed the events up to the given day
  def self.processed_until_date?(queue, date)
    if (events_exist?(queue))
      (queue.top.timestamp > date)? true : false
    else
      true
    end
  end

  def self.events_exist?(queue)
    (queue.nil? || queue.empty?)? false : true
  end

end
