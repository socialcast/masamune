require 'active_support'
require 'active_support/duration'
require 'active_support/values/time_zone'
require 'date'

class Masamune::DataPlanElem
  def initialize(rule, start_time, options = {})
    @rule = rule
    @start_time = start_time
    @options = options
  end

  def path
    if input_path && wildcard?
      input_path
    else
      start_time.strftime(@rule.pattern)
    end
  end

  def start_time
    @start_time
  end

  def stop_time
    @start_time.to_time.utc + @rule.time_step
  end

  def wildcard?
    @options.fetch(:wildcard, false)
  end

  def input_path
    @options[:input_path]
  end

  def next
    self.class.new(@rule, stop_time)
  end
end

class Masamune::DataPlanRule
  def initialize(pattern, options = {})
    @pattern = pattern
    @options = options
    @matcher = unbind_pattern(pattern)
  end

  def pattern
    @pattern
  end

  def matches?(input)
    @matcher.match(input) != nil
  end

  def bind_date(input_date)
    output_date = tz.utc_to_local(input_date)
    Masamune::DataPlanElem.new(self, output_date, @options)
  end

  def bind_path(input_path)
    matched_pattern = @matcher.match(input_path)
    raise "Cannot bind_path #{input_path} to #{@pattern}" unless matched_pattern
    output_date = matched_date(matched_pattern)
    Masamune::DataPlanElem.new(self, output_date, @options.merge(:input_path => input_path))
  end

  def unify_path(input_path, rule)
    matched_pattern = @matcher.match(input_path)
    raise "Cannot unify_path #{input_path} with #{rule.pattern}, does not match #{@pattern}" unless matched_pattern
    output_date = matched_date(matched_pattern)
    rule.bind_date(output_date)
  end

  def generate(start_time, stop_time, &block)
    if block_given?
      generate_with_block(start_time, stop_time, &block)
    else
      [].tap do |elems|
        generate_with_block(start_time, stop_time) do |elem|
          elems << elem
        end
      end
    end
  end

  # FIXME implement non 1:1 unification and substitution
  def generate_via_unify_path(input_path, rule, &block)
    if block_given?
      yield unify_path(input_path, rule)
    else
      [unify_path(input_path, rule)]
    end
  end

  def tz
    ActiveSupport::TimeZone[@options.fetch(:tz, 'UTC')]
  end

  def time_step
    case @pattern
    when /%k/, /%H/
      1.hour.to_i
    when /%d/
      1.day.to_i
    else
      1.day.to_i
    end
  end

  private

  def unbind_pattern(string)
    regexp = string.dup
    regexp.gsub!('%Y', '(?<year>\d{4})')
    regexp.gsub!('%m', '(?<month>\d{2})')
    regexp.gsub!('%-m', '(?<month>\d{1,2})')
    regexp.gsub!('%d', '(?<day>\d{2})')
    regexp.gsub!('%-d', '(?<day>\d{1,2})')
    regexp.gsub!('%H', '(?<hour>\d{2})')
    regexp.gsub!('%k', '(?<hour>\d{2})')
    regexp.gsub!('%-k', '(?<hour>\d{1,2})')
    regexp.gsub!('*', '.*?')
    Regexp.compile(regexp)
  end

  def matched_date(matched_pattern)
    matched_attrs = [:year, :month, :day, :hour].select { |x| matched_pattern.names.map(&:to_sym).include?(x) }
    DateTime.new(*matched_attrs.map { |x| matched_pattern[x].to_i })
  end

  def generate_with_block(start_time, stop_time, &block)
    instance = bind_date(start_time)
    begin
      yield instance
      instance = instance.next
    end while instance.start_time <= stop_time
  end
end