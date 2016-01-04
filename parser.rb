require_relative 'events/input_log_event'
require_relative 'events/control_event'
require_relative 'events_stream'
require_relative 'modules/constants'
require 'logger'

# Code intentionally doesn't check for incorrect input (For example null values on hostnames) for performance reasons. Nevertheless, any such case well be caught as an exception later during simulation execution
class Parser
  def initialize(file)
    @file = file
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::INFO
  end

  def parse_and_enqueue(date_filter_on, start_date = '-1', end_date = '9999999999', consider_clusters = '', ignore_clusters = '')
    return if @file.nil?
    if date_filter_on
      start_date = start_date.to_i
      end_date = end_date.to_i
      @logger.info "Requested start time: #{Time.at(start_date)} - Finish time: #{Time.at(end_date)}"
    else
      @logger.info 'No date filter! All dates on the file will be considered...'
    end

    @logger.info 'Loading events from file! This might take a while...'

    t1 = Time.now
    lines = File.readlines(@file) unless @file.nil?
    t2 = Time.now
    @logger.info "File loaded into memory in: #{(t2 - t1)} sec"

    t1 = Time.now if @logger.debug?
    @logger.info 'Formatting and filtering input lines...'
    lines = format_lines(lines)

    @logger.info "Number of lines: #{lines.size}" 

    t2 = Time.now  if @logger.debug?
    @logger.debug "Formatting done in #{t2 - t1} sec"

    t1 = Time.now
    @logger.info 'Creating events stream... This might take a while...'
    stream = create_events_queue(lines, date_filter_on, start_date, end_date, consider_clusters, ignore_clusters)
    t2 = Time.now
    @logger.info "Creation done in #{t2 - t1} sec"

    # Code block for inserting events as array on the pqueue - It's slower!
    #
    #        t1 = Time.now
    #        @logger.info "Creating events array... This might take a while..."
    #        events_as_array = create_events_array(lines, start_date, end_date, participating_clusters)
    #        t2 = Time.now
    #        @logger.info "Creating events done in #{t2-t1} sec"
    #
    #        #@logger.info "Adding array to priority queue..."
    #       t1 = Time.now
    #       stream = EventsStream.instance
    #       stream.add_sorted_array(events_as_array)
    #       t2 = Time.now
    #       @logger.info "Everything added into queue in #{t2-t1} sec"
    #

    stream
  end

  private

  def format_lines(lines)
    return if lines.nil?
    lines.map! do |l|
      begin
        l.chomp!
      rescue
        @logger.info "Problem with line: \"#{l}\""
        next
      end
    end
    lines
  end

  def create_event(event_attr)
    event = InputLogEvent.new(event_attr[0], event_attr[1], event_attr[2], event_attr[3], event_attr[4], event_attr[5])
  end

  def create_events_queue(lines, date_filter_on, start_date, end_date, consider_clusters = '', ignore_clusters = '', order = 'ASC')
    if (order == 'ASC')
      # Queue physical order is newest first - So we revert the input to make the order desc
      lines = lines.reverse
    end
    
    granularity = Constants::Numeric::GRANULARITY

    unless date_filter_on
      @logger.debug "first: #{lines.first} - last: #{lines.last}"
      start_date = lines.last.split("\t")[0].to_i
      end_date = lines.first.split("\t")[0].to_i
      @logger.debug "First date: #{Time.at(start_date)} (#{start_date}) - Last date: #{Time.at(end_date)} (#{end_date})"
    end

    stream = EventsStream.new
    lines_count = 0
    skipped_events_count = 0
    gap_events = 0

    last_timestamp = -1
    
    lines.each do |l|
      begin
        event_attr = l.split("\t")
        puts event_attr.inspect if @logger.debug?

        lines_count += 1
        timestamp = event_attr[0].to_i
        
        if timestamp >= start_date && timestamp <= end_date
          cluster_name = event_attr[1]
          if ignore_clusters.include?(cluster_name) || (!consider_clusters.empty? && !consider_clusters.include?(cluster_name))
            skipped_events_count += 1
            next
          end
          
          # Checking for problematic input
          # XXX Input is now fixed on dogfood - just replace the input file 
          # and remove check for performance
          if event_attr[3] == 'NULL'
            @logger.error "Null values on input!"
            event.print if @logger.error?
            skipped_events_count += 1
            next
          end

          event = create_event(event_attr)

          event.print if @logger.debug?
          
          # Generate events to fill up the input gaps
          if (last_timestamp != -1 && last_timestamp - timestamp > granularity)
            gap_timestamp = last_timestamp - granularity
            while(gap_timestamp - timestamp >= granularity)
              gap_events += 1
              gap_event = ControlEvent.new(gap_timestamp, Constants::ControlMessages::GAP)
              stream.push_without_reheap(gap_event)
              gap_timestamp -= granularity
            end
          end
          
          stream.push_without_reheap(event)
          last_timestamp = timestamp

        elsif timestamp > end_date
          skipped_events_count += 1
          next
        elsif timestamp < start_date
          skipped_events_count += 1
          break # Since the order (after reverting if needed) is DESC and the input monotonic we can stop here
        else
          skipped_events_count += 1
          puts l
        end
      rescue => e
        @logger.error e.message
        @logger.error "Problem with line #{lines_count} with content: \"#{l}\""
        # next
        raise
      end
    end
    @logger.info "Skipped events: #{skipped_events_count}"

    if (lines_count != (stream.size + skipped_events_count - gap_events))
      fail ParseError, "Size of processed input (= #{lines_count}) doesn't equal (number of events\
      created (= #{stream.size}) + events skipped (=#{skipped_events_count}) - gap_events(=#{gap_events}))"
    end
    stream
  end

end