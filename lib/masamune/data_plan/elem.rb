class Masamune::DataPlan::Elem
  MISSING_MODIFIED_AT = Time.new(0)

  include Masamune::Accumulate
  include Comparable

  attr_reader :rule, :options

  def initialize(rule, start_time, options = {})
    @rule = rule
    self.start_time = start_time
    @options = options
  end

  def input
    @input ||= start_time.strftime(strftime_format)
  end
  alias :path :input
  alias :table :input

  def partition
    input.split('_').last
  end
  alias :suffix :partition

  def exists?
    if rule.for_path?
      rule.engine.filesystem.exists?(path)
    elsif rule.for_table_with_partition?
      rule.engine.postgres_helper.table_exists?(table)
    elsif rule.for_table?
      table
    end
  end

  def complete?
    if rule.for_targets?
      sources.existing.map(&:start_date).uniq.length == sources.map(&:start_date).uniq.length
    end
  end

  def last_modified_at
    if rule.for_path?
      rule.engine.filesystem.stat(path).try(:mtime)
    elsif rule.for_table?
      rule.engine.postgres_helper.table_last_modified_at(table, @options)
    end || MISSING_MODIFIED_AT
  end

  def explode(&block)
    if rule.for_path?
      file_glob = path
      file_glob += '/' unless path.include?('*') || path.include?('.')
      file_glob += '*' unless path.include?('*')
      rule.engine.filesystem.glob(file_glob) do |new_path|
        yield rule.bind_input(new_path)
      end
    elsif rule.for_table_with_partition?
      yield table if rule.engine.postgres_helper.table_exists?(table)
    end
  end
  method_accumulate :explode

  def targets(&block)
    return Masamune::DataPlan::Set::EMPTY if @rule.for_targets?
    rule.engine.targets_for_source(rule.name, self) do |target|
      yield target
    end
  end
  method_accumulate :targets, lambda { |elem| Masamune::DataPlan::Set.new(elem.rule.engine.get_target_rule(elem.rule.name)) }

  def target
    targets.first
  end

  def sources(&block)
    return Masamune::DataPlan::Set::EMPTY if @rule.for_sources?
    rule.engine.sources_for_target(rule.name, self) do |source|
      yield source
    end
  end
  method_accumulate :sources, lambda { |elem| Masamune::DataPlan::Set.new(elem.rule.engine.get_source_rule(elem.rule.name)) }

  def source
    sources.first
  end

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

  def round(grain)
    self.class.new(@rule.round(grain), start_time, @options)
  end

  def ==(other)
    uniq_constraint == other.uniq_constraint
  end

  def eql?(other)
    self == other
  end

  def hash
    uniq_constraint.hash
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
    {rule: rule, input: input, start_date: start_time.to_s, stop_date: stop_time.to_s, :options => options}.to_s
  end

  protected

  def uniq_constraint
    [rule, options, rule.for_table? ? start_time : input]
  end

  private

  def strftime_format
    @strftime_format ||= glob ? @rule.strftime_format.sub('*', glob) : @rule.strftime_format
  end
end
