class Masamune::DataPlanSet < Set
  EMPTY = self.new

  attr_reader :rule

  def initialize(rule, enum = nil)
    @rule = rule
    super convert_enum(enum)
  end

  def union(enum = nil)
    super(convert_enum(enum) || EMPTY)
  end

  def add(elem = nil)
    super convert_elem(elem)
  end

  def include?(elem = nil)
    super convert_elem(elem)
  end

  def missing
    self.class.new(@rule).tap do |set|
      self.each do |elem|
        set.add elem if Masamune.filesystem.glob(elem.path).empty?
      end
    end
  end

  def existing
    self.class.new(@rule).tap do |set|
      self.each do |elem|
        # FIXME why does glob need to be array here
        Masamune.filesystem.glob(elem.path).each do |path|
          set.add elem.rule.bind_path(path)
        end
      end
    end
  end

  def adjacent
    self.class.new(@rule).tap do |set|
      self.each do |elem|
        @rule.adjacent_matches(elem) do |adj_elem|
          set.add adj_elem
        end
      end
    end
  end

  def actionable
    self.class.new(@rule).tap do |set|
      self.each do |elem|
        if @rule.type == :source
          set.add elem if elem.targets.existing.any?
        elsif @rule.type == :target
          set.add elem if elem.sources.existing.any?
        end
      end
    end
  end

  private

  def convert_elem(elem)
    case elem
    when nil
    when Masamune::DataPlanElem
      elem
    when String
      @rule.bind_path(elem)
    else
      raise "Unhandled elem class #{elem.class}"
    end
  end

  def convert_enum(enum)
    case enum
    when nil
    when Array
      enum.flatten.uniq
    when Set, self.class
      enum
    when String
      [enum]
    else
      raise "Unhandled enum class #{enum.class}"
    end
  end
end
