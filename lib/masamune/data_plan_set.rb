class Masamune::DataPlanSet < Set
  EMPTY = self.new

  def initialize(rule, enum = nil)
    @rule = rule
    super convert_enum(enum)
  end

  def union(enum = nil)
    super(enum || EMPTY)
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
        Masamune.filesystem.glob(elem.path) do |path|
          set.add elem.rule.bind_path(path)
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
