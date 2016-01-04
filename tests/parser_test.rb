require 'test/unit'
require '../parser'

# To make sure vim has tab delimited values use the following when creating an input:
#    :%retab!
#    :%substitute/ \+/\t/g
class ParserTest < Test::Unit::TestCase
  def setup
    @file = 'inputs/sample_input.csv'
    parser = Parser.new(@file)
    # lines = File.readlines(parser.file)
    # parser.send(:format_lines, lines)
    @stream = parser.parse_and_enqueue(false, '', '', '', '')

    @last_expected = ['1433116740', 'airpush', 'job_1430168648641_32487', \
      '109-05-03.as1.altiscale.com', 'ALLOCATED', '1']
    @first_expected = ['1430438400', 'dlx', 'job_1428607088671_4658', \
      '205-46.sc1.verticloud.com', 'REQUESTED', '15']
    @expectArrayElements = 44835
  end

  def test_parse_and_enqueue
    assert_nothing_raised do
      first_event = @stream.top
      last_event = @stream.bottom

      assert(first_event.timestamp == @first_expected[0].to_i, \
      "timestamp of 1st parsed line of file: #{@file} is #{first_event.timestamp} \
	     - Expecting: #{@first_expected[0].to_i}")
      assert(last_event.timestamp == @last_expected[0].to_i, \
      "timestamp of last parsed line of file: #{@file} is #{last_event.timestamp} \
	     - Expecting: #{@last_expected[0].to_i}")
      assert(@stream.size == @expectArrayElements, \
      "Parser stream elements are #{@stream.size} \
	     - Expecting: #{@expectArrayElements}")
      assert(first_event.cluster_name == @first_expected[1], \
      "cluster_name incorrect! Expecting: #{@first_expected[1]} - \
	     Found: #{first_event.cluster_name}")
      assert(first_event.job_id == @first_expected[2], \
      "job_id incorrect! Expecting: #{@first_expected[2]} -\
              Found: #{first_event.job_id}")
      assert(first_event.host_name == @first_expected[3], \
      "host_name incorrect! Expecting: #{@first_expected[3]} \
	     Found: #{first_event.host_name}")
      assert(first_event.task_status == @first_expected[4], \
      "task_status incorrect! Expecting: #{@first_expected[4]} - \
	     Found: #{first_event.task_status}")
      assert(first_event.task_count == @first_expected[5].to_i, \
      "task_count incorrect! Expecting: #{@first_expected[5].to_i} - \
             Found: #{first_event.task_count}")
      assert(last_event.cluster_name == @last_expected[1], \
      "cluster_name incorrect! Expecting: #{@last_expected[1]} - \
	     Found: #{last_event.cluster_name}")
      assert(last_event.job_id == @last_expected[2], \
      "job_id incorrect! Expecting: #{@last_expected[2]} - \
	     Found: #{last_event.job_id}")
      assert(last_event.host_name == @last_expected[3], \
      "host_name incorrect! Expecting: #{@last_expected[3]} -\
	     Found: #{last_event.host_name}")
      assert(last_event.task_status == @last_expected[4], \
      "task_status incorrect! Expecting: #{@last_expected[4]} -\
             Found: #{last_event.task_status}")
      assert(last_event.task_count == @last_expected[5].to_i, \
      "task_count incorrect! Expecting: #{@last_expected[5].to_i} -\
	     Found: #{last_event.task_count}")
    end
  end
end
