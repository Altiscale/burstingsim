require 'test/unit'

require_relative '../simulation'

# To make sure vim has tab delimited values use the following when creating an input:
#    :%retab!
#    :%substitute/ \+/\t/g

class PerfTest < Test::Unit::TestCase
  def setup
    @sim = Simulation.new('configs/default.yml')
  end

  def teardown
    @sim.clean_up
  end

  def test_replay_performance
    assert_nothing_raised do
      @sim.input_file = 'inputs/full_clusters2.csv'
      @sim.scheduler_config['policy']=(Constants::SchedulingPolicies::REPLAY)
      t1 = Time.now
      @sim.run
      t2 = Time.now
      delta = t2-t1
      puts t1
      puts t2
      puts delta
      #Alert in case processing time sky rockets because of changes
      puts "Elapsed time: #{delta}"
      assert(delta < 17, "Expected < 17 - Found #{delta}")
    end
  end

  def test_simple_performance
    assert_nothing_raised do
      @sim.input_file = 'inputs/full_clusters2.csv'
      @sim.scheduler_config['policy']=(Constants::SchedulingPolicies::SIMPLE)
      t1 = Time.now
      @sim.run
      t2 = Time.now
      delta = t2-t1
      puts t1
      puts t2
      puts delta
      #Alert in case processing time sky rockets because of changes
      puts "Elapsed time: #{delta}"
      assert(delta < 20, "Expected < 20 - Found #{delta}")
    end
  end
end