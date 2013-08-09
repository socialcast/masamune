require 'delegate'

class Masamune::DataPlanSet < Set
  require 'masamune/accumulate'
  include Masamune::Accumulate

  EMPTY = self.new

  def initialize(enum = nil)
    case enum
    when Array
      super enum.flatten.uniq
    when Set, self.class
      super
      merge(enum)
    else
      super Array.wrap(enum)
    end
  end

  def union(enum = nil)
    super(enum || EMPTY)
  end

  def missing(&block)
    self.each do |elem|
      yield elem if Masamune.filesystem.glob(elem.path).empty?
    end
  end
  method_accumulate :missing

  def existing(&block)
    self.each do |elem|
      Masamune.filesystem.glob(elem.path) do |path|
        yield elem.rule.bind_path(path)
      end
    end
  end
  method_accumulate :existing

  def inspect
    self.map(&:path)
  end
end
