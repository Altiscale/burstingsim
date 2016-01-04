require_relative 'pqueue'
require_relative 'events/input_log_event'

# The stream of events. Contains a priority queue with events ordered by their timestamps.
# It's interface provides decorators over the priority queue implementation
class EventsStream
  attr_accessor :queue

  def initialize
    @queue = PQueue.new([]) { |a, b| b <=> a } # Smaller timestamp -> Highest priority
  end

  def pop_and_print_all
    @queue.each_pop { |e| puts e.print }
  end

  def clear
    @queue.clear
  end
  def top
    @queue.top
  end

  def push_without_reheap(v)
    @queue.push_without_reheap(v)
  end

  def pop
    @queue.pop
  end

  # Returns the element with the lowest priority
  def bottom
    @queue.bottom
  end

  def init_with_sorted_array(elements)
    @queue.init_with_sorted_array(elements)
  end

  def concat(elements)
    @queue.concat(elements)
  end

  def push(element)
    @queue.push(element)
  end

  def inspect
    @queue.inspect
  end

  def size
    @queue.size
  end

  def empty?
    @queue.empty?
    end
end
