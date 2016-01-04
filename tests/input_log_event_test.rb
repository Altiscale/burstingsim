require 'test/unit'

require_relative '../events/input_log_event'

class InputLogEventTest < Test::Unit::TestCase
  def setup
    @expect_name = 'my_name'
    cluster_name = @expect_name
    job_id = 123
    host_name = 'myhost'
    task_status = 'EXPIRED'
    task_count = 123

    @expect_timestamp = 10_101_010
    @inputLogEvent = InputLogEvent.new(@expect_timestamp, cluster_name,\
    job_id, host_name, task_status, task_count)
  end

  def test_constructor
    assert(@inputLogEvent.timestamp == @expect_timestamp,\
    "Timestamp is not initialized correctly! \
	   Received: #{@inputLogEvent.timestamp} - Expected: #{@expect_timestamp}")
    assert(@inputLogEvent.cluster_name == @expect_name,\
    "Event attributes are not initialized correctly! \
	   Received: #{@inputLogEvent.cluster_name} - Expected: @expect_name")
  end
end
