require 'set'

# Time based LRU. Keeps a 1-minute window of events and pops everything else in order to be processed.
class Lru
  attr_reader :max_size, :ttl

  def initialize(*args)
    max_size, ttl, current_time = args

    ttl ||= :none

    fail ArgumentError.new(:max_size) if
        max_size < 1 && max_size != -1
    fail ArgumentError.new(:ttl) unless
        ttl == :none || ((ttl.is_a? Numeric) && ttl >= 0)

    @max_size = max_size #-1 for unlimited max size
    @ttl = ttl
    @data_lru = {}
    @current_time = 9_999_999_999_999_999
  end

  def max_size=(max_size)
    max_size ||= @max_size

    fail ArgumentError.new(:max_size) if
        max_size < 1 && max_size != -1

    @max_size = max_size

    resize
  end

  def ttl=(ttl)
    ttl ||= @ttl

    fail ArgumentError.new(:ttl) unless
        ttl == :none || ((ttl.is_a? Numeric) && ttl >= 0)

    @ttl = ttl

    pop_expired_keys
  end

  def [](key)
    found = true
    value = @data_lru.delete(key) { found = false }
    @data_lru[key] = value if found
  end

  def []=(key, val)

    @data_lru.delete(key)

    @data_lru[key] = val

    unless @max_size == -1
      if @data_lru.size > @max_size
        key, = @data_lru.first

        @data_lru.delete(key)
      end
    end

    val
  end

  def print
    array = @data_lru.to_a
    array.reverse!.each do |k, v|
      puts "#{k.inspect} => #{v}"
    end
  end

  def each
    array = @data_lru.to_a
    array.reverse!.each do |pair|
      yield pair
    end
  end
  
  # used further up the chain, non thread safe each
  alias_method :each_unsafe, :each

  def to_a
    array = @data_lru.to_a
    array.reverse!
  end

  def delete(key)
    @data_lru.delete(key)
  end

  alias_method :evict, :delete

  def key?(key)
    @data_lru.key?(key)
  end

  alias_method :has_key?, :key?

  def clear
    @data_lru.clear
  end

  def expire(timestamp)
    @current_time = timestamp
    pop_expired_keys
  end

  def count
    @data_lru.size
  end

  def empty?
    @data_lru.empty?
  end

  protected

  def pop_expired_keys
    return if @ttl == :none || @data_lru.empty?

    ttl_horizon = @current_time - @ttl
    key, time = @data_lru.first
    return unless time < ttl_horizon

    s = Set.new {}
    until time.nil? || time >= ttl_horizon
      @data_lru.delete(key)
      s.add(key)
      key, time = @data_lru.first
    end
    s
  end

  def resize
    s = pop_expired_keys

    unless @max_size == -1
      while @data_lru.size > @max_size
        key, = @data_lru.first

        @data_lru.delete(key)
      end
    end
    s
  end
end
