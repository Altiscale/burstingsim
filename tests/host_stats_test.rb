require 'test/unit'

require_relative '../simulation'

# To make sure vim has tab delimited values use the following when creating an input:
#    :%retab!
#    :%substitute/ \+/\t/g
class HostStatsTest < Test::Unit::TestCase
  def setup
    @sim = Simulation.new('configs/default.yml')
    @sim.scheduler_config['policy'] = Constants::SchedulingPolicies::REPLAY
  end

  def teardown
    @sim.clean_up
  end

  def test_hosts_stats_simple
    @sim.input_file = 'inputs/busy_time_simple.csv'
    @sim.run

    # busy count is 0 because there is not state change
    set_expected_host_stats([180, -1, 180, -1, 180, 0, 1, 0, 'BUSY', 1_431_896_340])
    get_sim_host_stats('105-32.sc1.verticloud.com', 1_431_896_340)
    assert_host_stats_values
  end

  def test_hosts_stats_changed_from_requested
    @sim.input_file = 'inputs/busy_time_changed_from_requested.csv'
    @sim.run
    # busy count is 0 because there is not state change
    set_expected_host_stats([300, -1, 300, -1, 300, 0, 1, 0, 'BUSY', 1_431_896_220])
    get_sim_host_stats('105-32.sc1.verticloud.com', 1_431_896_520)
    assert_host_stats_values
  end

  def test_hosts_stats_no_gap
    @sim.input_file = 'inputs/busy_time_no_gap.csv'
    @sim.run

    set_expected_host_stats([180, 120, 180, 120, 360, 120, 2, 1, 'BUSY', 1_431_896_520])
    get_sim_host_stats('105-32.sc1.verticloud.com', 1_431_896_640)
    assert_host_stats_values
  end

  def test_hosts_stats_dif_max_min
    @sim.input_file = 'inputs/busy_time_diff_max_mix.csv'
    @sim.run

    set_expected_host_stats([180, 120, 180, 120, 360, 180, 2, 2, 'FREE', 1_431_896_700])
    get_sim_host_stats('105-32.sc1.verticloud.com', 1_431_896_700)
    assert_host_stats_values
   
  end

  def test_hosts_stats_with_gap
    @sim.input_file = 'inputs/busy_time_with_gap.csv'
    @sim.run

    set_expected_host_stats([180, 120, 180, 120, 360, 120, 2, 1, 'BUSY', 1_431_896_520])
    get_sim_host_stats('105-32.sc1.verticloud.com', 1_431_896_640)
    assert_host_stats_values
  end

  private

  def set_expected_host_stats(exp_host_stats)
    @exp_max_time_busy = exp_host_stats[0]
    @exp_max_time_free = exp_host_stats[1]
    @exp_min_time_busy = exp_host_stats[2]
    @exp_min_time_free = exp_host_stats[3]
    @exp_total_time_busy = exp_host_stats[4]
    @exp_total_time_free = exp_host_stats[5]
    @exp_busy_count = exp_host_stats[6]
    @exp_free_count = exp_host_stats[7]
    @exp_current_status = exp_host_stats[8]
    @exp_last_status_change = exp_host_stats[9]
  end

  def get_sim_host_stats(host_name, timestamp)
    @stats = @sim.sim_state.world.hosts[host_name].get_updated_stats(timestamp)
  end

  def assert_host_stats_values
    # TODO: Make lines length smaller
    assert(@stats.max_time_busy == @exp_max_time_busy, \
    "Incorrect max_time_busy! Expecting: #{@exp_max_time_busy} \
	   Found: #{@stats.max_time_busy}")
    assert(@stats.max_time_free == @exp_max_time_free, \
    "Incorrect max_time_free! Expecting: #{@exp_max_time_free} \
	   Found: #{@stats.max_time_free}")
    assert(@stats.min_time_busy == @exp_min_time_busy, \
    "Incorrect min_time_busy! Expecting: #{@exp_min_time_busy} \
	   Found: #{@stats.min_time_busy}")
    assert(@stats.min_time_free == @exp_min_time_free, \
    "Incorrect min_time_free! Expecting: #{@exp_min_time_free} \
	   Found: #{@stats.min_time_free}")
    assert(@stats.total_time_busy == @exp_total_time_busy, \
    "Incorrect total_time_busy! Expecting: #{@exp_total_time_busy} \
	   Found: #{@stats.total_time_busy}")
    assert(@stats.total_time_free == @exp_total_time_free, \
    "Incorrect total_time_free! Expecting: #{@exp_total_time_free} \
	   Found: #{@stats.total_time_free}")
    assert(@stats.busy_count == @exp_busy_count, \
    "Incorrect busy_count! Expecting: #{@exp_busy_count} \
           Found: #{@stats.busy_count}")
    assert(@stats.free_count == @exp_free_count, \
    "Incorrect free_count! Expecting: #{@exp_free_count} \
	   Found: #{@stats.free_count}")
    assert(@stats.current_status == @exp_current_status, \
    "Incorrect current_status! Expecting: #{@exp_current_status} \
	   Found: #{@stats.current_status}")
  end
end
