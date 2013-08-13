class Masamune::DataPlanSet < Set
  EMPTY = self.new

  include Masamune::Accumulate

  attr_reader :rule

  def initialize(rule, enum = nil)
    @rule = rule
    super convert_enum(enum)
  end

  def type
    @rule.type
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

  def missing(&block)
    self.each do |elem|
      yield elem if Masamune.filesystem.glob(elem.path).empty?
    end
  end
  method_accumulate :missing, lambda { |set| set.class.new(set.rule) }

  def existing(&block)
    self.each do |elem|
      # FIXME why does glob need to be array here
      Masamune.filesystem.glob(elem.path).each do |path|
        yield elem.rule.bind_path(path)
      end
    end
  end
  method_accumulate :existing, lambda { |set| set.class.new(set.rule) }

  def adjacent(&block)
    self.each do |elem|
      @rule.adjacent_matches(elem) do |adj_elem|
        yield adj_elem
      end
    end
  end
  method_accumulate :adjacent, lambda { |set| set.class.new(set.rule) }

  def actionable(&block)
    self.each do |elem|
      if type == :source
        yield elem if elem.targets.existing.any?
      elsif type == :target
        yield elem if elem.sources.existing.any?
      end
    end
  end
  method_accumulate :actionable, lambda { |set| set.class.new(set.rule) }

  def targets
    return Masamune::DataPlanSet::EMPTY if empty? || type == :target
    self.class.new(self.first.targets.rule).tap do |set|
      self.each do |elem|
        set.merge elem.targets
      end
    end
  end

  def sources
    return Masamune::DataPlanSet::EMPTY if empty? || type == :source
    self.class.new(self.first.sources.rule).tap do |set|
      self.each do |elem|
        set.merge elem.sources
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
