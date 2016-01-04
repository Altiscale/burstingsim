require 'yaml'
require 'logger'

require_relative 'stream_generator'
require_relative 'world_state'
require_relative 'events_stream'
require_relative 'state_updater'
require_relative 'trace'
require_relative 'modules/errors'
require_relative 'modules/constants'
require_relative 'events/control_event'
require_relative 'scheduler'

# This is the class controlling the simulation. 
# Gets events from the stream generator, sends them to the state_updater and prints the stats 
# at the end of the simulation. The simulation model is located to the world.rb class containing 
# collocs, clusters, hosts, jobs etc
class Simulation
  attr_reader :training_window,
              :speed,
              :sim_granulatiry,
              :date_filter_on,
              :req_start_date, # Requested start date
              :req_end_date,
              :real_speed, # events/sec processed (not including training window)
              :elapsed_time,
              :window_events_count,
              :sim_events_count,
              :training_window_start_time,
              :training_window_end_time,
              :real_start_time,
              :real_end_time,
              :sim_state,
              :scheduler

  attr_accessor :input_file, 
                :scheduler_config,
                :world_config

  def initialize(config_file)
    config = YAML.load_file(config_file)
    @input_file = config['input_file']
    puts "input: #{@input_file}"
    @training_window = config['training_window']
    @speed = config['speed'] * 3600.0 # TODO: NOT USED
    @sim_granulatiry = config['sim_granulatiry'] # TODO: NOT USED
    req_start_date = config['start_date'].split('-')
    req_end_date = config['end_date'].split('-')
    @date_filter_on = config['date_filter_on']
    @req_start_date = Time.new(req_start_date[0], req_start_date[1], req_start_date[2]).to_i
    @req_end_date = Time.new(req_end_date[0], req_end_date[1], req_end_date[2]).to_i
    @ignore_clusters = create_set_from_config_string(config['ignore_clusters'])
    @consider_clusters = create_set_from_config_string(config['consider_clusters'])
    @elapsed_time = 0
    @sim_events_count = 0
    @window_events_count = 0
    @training_window_start_time = 0
    @training_window_end_time = 0
    @real_start_time = 0
    @real_end_time = 0
    @elapsed_time = 0
    @world_config = {}
    @world_config['inventory_hosts_count'] = config['inventory_hosts_count']  
    @scheduler_config = {}  
    @scheduler_config['eviction_delay']  = config['eviction_delay']
    @scheduler_config['provision_delay']  = config['provision_delay']
    @scheduler_config['scheduling_granularity']  = config['scheduling_granularity']
    @scheduler_config['allow_evictions'] = config['allow_evictions']
    @scheduler_config['allow_provisions'] = config['allow_provisions']
    @scheduler_config['policy'] = config['policy']
    @verbosity = config['verbosity']  
   
      
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::INFO
    if @ignore_clusters <= @consider_clusters && !@ignore_clusters.empty? 
	    @logger.warn 'WARNING! Clusters on ignore set also on consider set!' 
    end

    puts 'Considering clusters in set:'
    puts((@consider_clusters.empty?) ? 'All' : @consider_clusters.inspect)
    puts 'Ignoring clusters in set:'
    puts((@ignore_clusters.empty?) ? 'None' : @ignore_clusters.inspect)
  end

  def print_parameters
    instance_variables.each { |k, _v| puts "#{k}: #{instance_variable_get(k)}" }
  end

  def clean_up
    StateUpdater.empty_events
  end
  
  def print_summary
    @logger.info 'Simulation Done!'
    @logger.info "Training window From: #{Time.at(@training_window_start_time)} \
    - To: #{Time.at(@training_window_end_time)} (#{@training_window_start_time} \
    - #{@training_window_end_time})"
    @logger.info "Simulated dates From: #{Time.at(@real_start_time)} \
    - To: #{Time.at(@real_end_time)} (#{@real_start_time} - #{@real_end_time})"
    @logger.info "Elapsed time: #{@elapsed_time} seconds - #{@elapsed_time / 60} min"
    @logger.info "Training window events: #{@window_events_count}"
    @logger.info "Events processed: #{@sim_events_count} (Events/sec: #{@real_speed.to_i})"
  end

  def run
    @logger.info '*CREATING STREAM GENERATOR FROM INPUT*'
    stream_generator = StreamGenerator.new(@input_file, @speed, @date_filter_on, @req_start_date\
					   , @req_end_date, @consider_clusters, @ignore_clusters)
		stream = stream_generator.get_stream
    state = WorldState.new(@world_config)

    @scheduler = Scheduler.new(state, @scheduler_config)
    @logger.info '*SIMULATION STARTING*'
    @logger.info 'Creating initial state by applying window events...'
    @logger.info 'Getting events for window...'
    t1 = Time.now

      t2 = Time.now
      elapsed_time = t2 - t1
      t1 = Time.now
      
    if(stream.empty?)
      @logger.fatal '
      Load a bigger file or check the requested start-end dates in configuration!
      I have no events to start simulation!'
      @real_start_time = @training_window_start_time
      @real_end_time = @training_window_end_time
      state.print_summary
      state.print_all
      finish_simulation(state)
      return
    end
 
      @training_window_start_time = stream.top.timestamp 
      
      StateUpdater.set_stream(stream)
      state, @window_events_count = StateUpdater.process_stream(state, @training_window)
      t2 = Time.now
      elapsed_time = t2 - t1
      @logger.info "Initial state created! \
      Elapsed time: #{elapsed_time.to_i} sec \
      Events/sec: #{(@window_events_count / elapsed_time).to_i}"
      @logger.debug "Initial state time: #{state.timestamp.to_i} - #{Time.at(state.timestamp.to_i)}"
      @logger.debug 'Sleeping long enough so you can read the stats above...'
      sleep 3 if @logger.debug?

      @training_window_end_time = state.timestamp()

     unless(!stream.empty?)
        @logger.fatal 'Load a bigger file or check the requested start-end dates in configuration! \
        I have no events to continue simulation after processing the window!'
        @real_start_time = @training_window_start_time
        @real_end_time = @training_window_end_time
        state.print_summary
        state.print_all
        finish_simulation(state)
        return
      end
        
    t1 = Time.now
    @logger.info 'Processing stream...'
    @real_start_time = stream.top.timestamp
    state, @sim_events_count = StateUpdater.process_stream(state)
    @real_end_time = state.timestamp()
    t2 = Time.now
    @elapsed_time = t2 - t1
    @real_speed = @sim_events_count / @elapsed_time
    finish_simulation(state)
    nil
  end

  private

  def finish_simulation(state)
    @logger.debug 'Sending simulation is over event!'
    begin
      # Create new event with timestamp 1 min greater than last processed event and finish simulation
      state = StateUpdater.apply_event(state, ControlEvent.new(state.timestamp \
		+ Constants::Numeric::GRANULARITY, Constants::ControlMessages::SIM_IS_OVER))
      state.print_summary
      state.print_all
      @sim_state = state # used to allow access from unit tests
    rescue => e
      puts e
    end
  end

  def create_set_from_config_string(string)
    set = Set.new
    string.scan(/\w+/).each { |cluster| set.add(cluster) }
    set
  end
end

if __FILE__ == $PROGRAM_NAME
  sim = Simulation.new('config.yml')
  # sim.print_parameters
  sim.run
  sim.print_summary
end
