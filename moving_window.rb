# Used to keep track of data for a specified size, creating window sums and averages of the data it keeps
class MovingWindow
  attr_reader :size

  def initialize(size)
    @size = size
    @buffer = []
  end

  def full?
    @buffer.size >= size
  end

  def emtpy?
    @buffer.empty?
  end

  def write(data)
    @buffer.shift if full?
    @buffer << data
  end

  def clear
    @buffer.clear
  end

  def sum
    @buffer.reduce(:+)
  end

  def avg
    @buffer.sum / @buffer.size
  end
end
