require 'yaml'

require_relative 'parser'
require_relative 'events/input_log_event'
require_relative 'events_stream'

# Calls parser to parse the input file and enqueue the events to a priority queue. 
# Provides interface to be called by the simulation object in order to send the events 
# from the priority queue.
class StreamGenerator
  attr_accessor :speed,
                :stream

  def initialize(input_file, speed, date_filter_on, start_date, end_date, consider_clusters, ignore_clusters)
    @inputFile = input_file
    @speed = speed
    @date_filter_on = date_filter_on
    @start_date = start_date
    @end_date = end_date
    @consider_clusters = consider_clusters
    @ignore_clusters = ignore_clusters
    
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::DEBUG

    parser = Parser.new(@inputFile)
    
    if @inputFile.nil?
      @logger.fatal "No input file specified!"
      exit
    end

    @logger.info 'Adding parsed events into priority queue...'
    t1 = Time.now
    @stream = EventsStream.new
    @stream = parser.parse_and_enqueue(date_filter_on, start_date, end_date, consider_clusters, ignore_clusters)
    t2 = Time.now
    @logger.info "Adding to priority queue done! Total events: #{@stream.size}"
    @logger.info "Elapsed time: #{(t2 - t1).to_i} sec"
    unless size < 3
    @logger.info "First event time: #{@stream.top.timestamp} (#{Time.at(@stream.top.timestamp)} \
    - Last: #{@stream.bottom.timestamp} (#{Time.at(@stream.bottom.timestamp)})"
    end
  end
  
  def get_stream
    @stream
  end
  
  def pop_all_events
    until @stream.empty?
         @stream.pop.print
    end
    #puts @stream.pop_and_print_all
  end

  def has_next
    return true unless @stream.empty?
  end

  def check_next
    @stream.top
  end

  def get_next
    @stream.pop
  end

  def is_empty?
    (size == 0)
  end

  def size
    @stream.size
  end

  def get_events_of_days(window)
    # * Method is obsolete
    get_events(window * (60 * 60 * 24))
  end

  
  def get_all_events
    # * Method is obsolete
      events = EventsStream.new
      until @stream.empty?
        event = @stream.pop
        event.print if @logger.debug?
        #events.push_without_reheap(event)
        #events.push(event)
      end
      events
    end

  def get_events(window)
     # * Method is obsolete
     fail ArgumentError, 'Window cannot be zero' unless window > 0
     fail ArgumentError, 'Window is not numeric' unless window.is_a? Numeric
     fail NoEventsInQueueError, 'Stream generator has no events in the queue' unless has_next
 
     start_date = @stream.bottom.timestamp
 
     end_date = start_date + window
 
     @logger.info "Window start: #{start_date} - end: #{end_date} \
     or #{Time.at(start_date)} - #{Time.at(end_date)}"
     events = EventsStream.new
     until @stream.empty? || @stream.top.timestamp > end_date
       event = @stream.pop
       event.print if @logger.debug?
       events.push_without_reheap(event) # Requires events are ordered in desc!
     end
     events
   end
end