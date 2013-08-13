class Masamune::DataPlanElem
  include Masamune::Accumulate
  include Comparable

  attr_reader :rule, :options

  def initialize(rule, start_time, options = {})
    @rule = rule
    self.start_time = start_time
    @options = options
  end

  def type
    @rule.type
  end

  def path
    if glob
      start_time.strftime(@rule.pattern.sub('*', glob))
    else
      start_time.strftime(@rule.pattern)
    end
  end

  def exists?
    Masamune.filesystem.exists?(path)
  end

  def targets(&block)
    return Masamune::DataPlanSet::EMPTY if type == :target
    rule.plan.targets_for_source(rule.name, self) do |target|
      yield target
    end
  end
  method_accumulate :targets, lambda { |elem| Masamune::DataPlanSet.new(elem.rule.plan.get_target_rule(elem.rule.name)) }

  def sources(&block)
    return Masamune::DataPlanSet::EMPTY if type == :source
    rule.plan.sources_for_target(rule.name, self) do |source|
      yield source
    end
  end
  method_accumulate :sources, lambda { |elem| Masamune::DataPlanSet.new(elem.rule.plan.get_source_rule(elem.rule.name)) }

  def start_time
    @start_time.to_time.utc
  end

  def start_time=(start_time)
    @start_time =
    case start_time
    when Time
      rule.time_round(start_time.utc)
    when Date, DateTime
      rule.time_round(start_time.to_time.utc)
    end
  end

  def start_date
    @start_time.to_date
  end

  def stop_time
    start_time.advance(@rule.time_step => 1)
  end

  def stop_date
    stop_time.to_date
  end

  def glob
    @options[:glob]
  end

  def next(i = 1)
    self.class.new(@rule, start_time.advance(@rule.time_step => +1*i), @options)
  end

  def prev(i = 1)
    self.class.new(@rule, start_time.advance(@rule.time_step => -1*i), @options)
  end

  def ==(other)
    rule == other.rule &&
    options == other.options &&
    start_time == other.start_time
  end

  def eql?(other)
    self == other
  end

  def hash
    [rule, options, start_time].hash
  end

  # FIXME should consider stop_time for correctness
  def <=>(other)
    if start_time < other.start_time
      1
    elsif start_time > other.start_time
      -1
    elsif start_time == other.start_time
      0
    end
  end

  def inspect
    {rule: rule, path: path, start_date: start_time.to_s, stop_date: stop_time.to_s, :options => options}.to_s
  end
end
