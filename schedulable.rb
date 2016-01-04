class Schedulable
  include Comparable
    
    attr_accessor :name,
      :score
    
    def <=> (another_schedulable)
      @score <=> another_schedulable.score
    end
    
    def initialize(name, score)
      @name = name
      @score = score
    end
    
    def print
      puts "#{name} - #{score}"
    end
end