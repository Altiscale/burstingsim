require 'test/unit'
require_relative 'parser_test'
require_relative 'input_log_event_test'
require_relative 'host_stats_test'
require_relative 'world_test'
require_relative 'events_stream_test'
require_relative 'perf_test'
# XXX: Z prefix intentionally added because test creates side-effects to other tests
# TODO: Add suitable tear-down to avoid above problem
require_relative 'scheduler_integration_test'


# Necessary class to run all tests from the above included test classes 
# as 'ruby test-suit.rb'
class TestSuit

end
