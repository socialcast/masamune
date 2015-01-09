require 'set'

class Masamune::DataPlanSet < Set
  EMPTY = self.new

  include Masamune::Accumulate

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

  def missing(&block)
    self.each do |elem|
      yield elem if elem.explode.empty?
    end
  end
  method_accumulate :missing, lambda { |set| set.class.new(set.rule) }

  def existing(&block)
    self.each do |elem|
      elem.explode do |new_elem|
        yield new_elem
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

  def stale(&block)
    return Masamune::DataPlanSet::EMPTY if empty? || @rule.for_sources?
    self.each do |target|
      yield target if target.sources.existing.any? { |source| target_stale?(source, target) }
    end
  end
  method_accumulate :stale, lambda { |set| set.class.new(set.rule) }

  def incomplete(&block)
    return Masamune::DataPlanSet::EMPTY if empty? || @rule.for_sources?
    set = Set.new
    self.each do |target|
      yield target if set.add?(target) unless target.complete?
    end
  end
  method_accumulate :incomplete, lambda { |set| set.class.new(set.rule) }

  def actionable(&block)
    return Masamune::DataPlanSet::EMPTY if empty? || @rule.for_sources?
    set = Set.new
    missing.each do |target|
      yield target if set.add?(target)
    end
    incomplete.each do |target|
      yield target if set.add?(target)
    end
    stale.each do |target|
      yield target if set.add?(target)
    end
  end
  method_accumulate :actionable, lambda { |set| set.class.new(set.rule) }

  def updateable(&block)
    return Masamune::DataPlanSet::EMPTY if empty? || @rule.for_sources?
    set = Set.new
    actionable.each do |target|
      yield target if set.add?(target) && target.sources.existing.any?
    end
  end
  method_accumulate :updateable, lambda { |set| set.class.new(set.rule) }

  # TODO detect & warn or correct if coarser grain set is incomplete
  def with_grain(grain, &block)
    seen = Set.new
    self.each do |elem|
      granular_elem = elem.round(grain)
      yield granular_elem if seen.add?(granular_elem)
    end
  end
  method_accumulate :with_grain, lambda { |set, grain| set.class.new(set.rule.round(grain)) }

  def targets
    return Masamune::DataPlanSet::EMPTY if empty? || @rule.for_targets?
    self.class.new(self.first.targets.rule).tap do |set|
      self.each do |elem|
        set.merge elem.targets
      end
    end
  end

  def sources
    return Masamune::DataPlanSet::EMPTY if empty? || @rule.for_sources?
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
      @rule.bind_input(elem)
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

  def target_stale?(source, target)
    target.last_modified_at != Masamune::DataPlanElem::MISSING_MODIFIED_AT &&
    source.last_modified_at != Masamune::DataPlanElem::MISSING_MODIFIED_AT &&
    source.last_modified_at >= target.last_modified_at
  end
end
