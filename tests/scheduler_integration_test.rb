require 'test/unit'

require_relative '../simulation'

# To make sure vim has tab delimited values use the following when creating an input:
#    :%retab!
#    :%substitute/ \+/\t/g

=begin
Configuration from tests/configs/default.yml

inventory_hosts_count: 10
min_inventory_hosts: 3
eviction_delay: 2
provision_delay: 2
scheduling_granularity: 60
=end

# IMPORTANT: At the time of testing constant values used for:
# avg_task_mem = 1536
# avg_task_time = 25020
# TODO(Stratos) yarn scheduler class should be modified accordingly to allow setting these variables
# Otherwise tests will break when these values are dynamically set from input files.

class SchedulerIntegrationTest < Test::Unit::TestCase
  def setup
    @sim = Simulation.new('configs/default.yml')
    @sim.scheduler_config['policy'] = Constants::SchedulingPolicies::SIMPLE
  end

  def teardown
    @sim.clean_up
  end

  def test_provisions_1
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions1.csv'
      @sim.run

      set_expected_count_values([1, 1, 7, 2, 1, 1, 1, 0, 1, 1, 1])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_2
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions2.csv'
      @sim.run

      set_expected_count_values([1, 2, 7, 1, 1, 2, 2, 0, 1, 0, 2])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_3
    # Tasks not fitting in only one host and 2nd host just got provisioned
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions3.csv'
      @sim.run

      set_expected_count_values([1, 2, 6, 2, 1, 2, 2, 0, 1, 29, 30])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_3b
    # Tasks not fitting in only one host completes on 1st provisioned host and rest of tasks
    # assigned to another provisioned host
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions3b.csv'
      @sim.run

      set_expected_count_values([2, 2, 6, 1, 2, 2, 3, 0, 1, 0, 30])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_3c
    # Waiting tasks equal capacity. Job is finished.
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions3c.csv'
      @sim.run

      set_expected_count_values([4, 0, 7, 0, 4, 0, 3, 0, 0, 0, 0])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_3d
    # Waiting tasks equal capacity. Job is finished. Another starts.
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions3d.csv'
      @sim.run

      set_expected_count_values([2, 2, 7, 0, 2, 2, 3, 0, 1, 0, 39])
      # test task assigment on inventory hosts
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_3e
    # Waiting tasks equal capacity. Job is finished and two new jobs start running on same minute
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions3e.csv'
      @sim.run

      set_expected_count_values([2, 2, 7, 0, 2, 2, 3, 0, 2, 0, 49])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_3f
    # Waiting tasks equal capacity. Job is finished and two new jobs start running next minute
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions3f.csv'
      @sim.run

      set_expected_count_values([0, 4, 7, 0, 0, 4, 3, 0, 2, 0, 49])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  # TODO: To run the following tests we first need to be able to configure avg_task_runtime to
  # avoid breaking all the other tests that assume job run time less than 1 minute
  # Test with larger runtimes have been performed manually though

  # TODO: Augmented job for more than 1 minute
  # TODO: Augmented job finishes but there are no-augmented tasks still running

  def test_provisions_4
    # Assignment of multiple tasks per host - fitting exactly to its capacity
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions4.csv'
      @sim.run

      set_expected_count_values([1, 2, 7, 1, 1, 2, 2, 0, 1, 0, 30])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_5
    # Assignment of multiple tasks per host - one more than capacity
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions5.csv'
      @sim.run

      set_expected_count_values([1, 2, 6, 2, 1, 2, 2, 0, 1, 1, 30])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_6
    # Assignment of multiple tasks per host - one less than capacity
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions6.csv'
      @sim.run

      set_expected_count_values([1, 2, 7, 1, 1, 2, 2, 0, 1, 0, 29])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values
    end
  end

  def test_provisions_7
    # Error case deriving from actual trace. Possible reason the reducing of job task counters
    # for a job that is already forced to finish because done on the provisioned hosts and
    # its task status counters are reseted and shouldn't be reduced more from other finished signals
    assert_nothing_raised do
      @sim.scheduler_config['allow_evictions'] = false
      @sim.input_file = 'inputs/provisions7.csv'
      @sim.world_config['inventory_hosts_count'] = 100
      @sim.run
    end
  end
  
  def test_evictions_1
    assert_nothing_raised do
       @sim.scheduler_config['allow_evictions'] = true
       @sim.scheduler_config['allow_provisions'] = true
       @sim.input_file = 'inputs/evictions1.csv'
       @sim.run
       
      set_expected_count_values([0, 2, 7, 2, 0, 2, 1, 0, 1, 0, 29])
      get_sim_count_values("sc1", "yellowhammer")
      assert_count_values      
     end
  end
  
  def test_evictions_2
     # Waiting tasks equal capacity. Job is finished.
     assert_nothing_raised do
       @sim.scheduler_config['allow_evictions'] = true
       @sim.scheduler_config['allow_provisions'] = true
       @sim.input_file = 'inputs/provisions3c.csv'
       @sim.run
  
       set_expected_count_values([1, 0 , 7, 3, 1, 0, 0, 0, 0, 0, 0])
       get_sim_count_values("sc1", "yellowhammer")
       assert_count_values
     end
   end

  private

  def set_expected_count_values(exp_counts)
    @exp_colloc_free_hosts_count = exp_counts[0]
    @exp_colloc_busy_hosts_count = exp_counts[1]
    @exp_colloc_inventory_hosts_count = exp_counts[2]
    @exp_colloc_transitioning_hosts_count = exp_counts[3]

    @exp_cluster_free_hosts_count = exp_counts[4]
    @exp_cluster_busy_hosts_count = exp_counts[5]
    @exp_cluster_provisioned_hosts_count = exp_counts[6]

    @exp_cluster_waiting_jobs = exp_counts[7]
    @exp_cluster_active_jobs = exp_counts[8]
    @exp_cluster_waiting_tasks = exp_counts[9]
    @exp_cluster_active_tasks = exp_counts[10]
  end

  def get_sim_count_values(colloc_name, cluster_name)
    colloc = @sim.sim_state.world.collocs[colloc_name]
    cluster = @sim.sim_state.world.clusters[cluster_name]

    @found_colloc_free_hosts_count = colloc.free_hosts_count
    @found_colloc_busy_hosts_count = colloc.busy_hosts_count
    @found_colloc_inventory_hosts_count = colloc.inventory_hosts_count
    @found_colloc_transitioning_hosts_count = colloc.transitioning_hosts_count

    @found_cluster_free_hosts_count = cluster.free_hosts_count
    @found_cluster_busy_hosts_count = cluster.busy_hosts_count
    @found_cluster_provisioned_hosts_count = cluster.provisioned_hosts_count

    @found_cluster_waiting_jobs = cluster.waiting_jobs_count
    @found_cluster_active_jobs = cluster.active_jobs_count
    @found_cluster_waiting_tasks = cluster.waiting_tasks_count
    @found_cluster_active_tasks = cluster.active_tasks_count
  end

  def assert_count_values
    assert(@found_colloc_free_hosts_count == @exp_colloc_free_hosts_count, \
    "colloc free hosts count incorrect! Expecting: #{@exp_colloc_free_hosts_count} \
      Found: #{@found_colloc_free_hosts_count}")
    assert(@found_colloc_busy_hosts_count == @exp_colloc_busy_hosts_count, \
    "colloc busy hosts count incorrect! Expecting: #{@exp_colloc_busy_hosts_count} \
      Found: #{@found_colloc_busy_hosts_count}")
    assert(@found_colloc_inventory_hosts_count == @exp_colloc_inventory_hosts_count, \
    "colloc inventory hosts count incorrect! Expecting: #{@exp_colloc_inventory_hosts_count} \
           Found: #{@found_colloc_inventory_hosts_count}")
    assert(@found_colloc_transitioning_hosts_count == @exp_colloc_transitioning_hosts_count, \
    "colloc transitioning hosts count incorrect! Expecting: #{@exp_colloc_transitioning_hosts_count} \
           Found: #{@found_colloc_transitioning_hosts_count}")

    assert(@found_cluster_free_hosts_count == @exp_cluster_free_hosts_count, \
    "cluster free hosts count incorrect! Expecting: #{@exp_cluster_free_hosts_count} \
      Found: #{@found_cluster_free_hosts_count}")
    assert(@found_cluster_busy_hosts_count == @exp_cluster_busy_hosts_count, \
    "cluster busy hosts count incorrect! Expecting: #{@exp_cluster_busy_hosts_count} \
      Found: #{@found_cluster_busy_hosts_count}")
    assert(@found_cluster_provisioned_hosts_count == @exp_cluster_provisioned_hosts_count, \
    "cluster provisioned hosts count incorrect! Expecting: #{@exp_cluster_provisioned_hosts_count} \
      Found: #{@found_cluster_provisioned_hosts_count}")

    assert(@found_cluster_waiting_jobs == @exp_cluster_waiting_jobs, \
    "cluster waiting jobs incorrect! Expecting: #{@exp_cluster_waiting_jobs} \
      Found: #{@found_cluster_waiting_jobs}")
    assert(@found_cluster_active_jobs == @exp_cluster_active_jobs, \
    "cluster active jobs incorrect! Expecting: #{@exp_cluster_active_jobs} \
      Found: #{@found_cluster_active_jobs}")
    assert(@found_cluster_waiting_tasks == @exp_cluster_waiting_tasks, \
    "cluster waiting tasks incorrect! Expecting: #{@exp_cluster_waiting_tasks} \
      Found: #{@found_cluster_waiting_tasks}")
    assert(@found_cluster_active_tasks == @exp_cluster_active_tasks, \
    "cluster active tasks incorrect! Expecting: #{@exp_cluster_active_tasks} \
      Found: #{@found_cluster_active_tasks}")
  end
end