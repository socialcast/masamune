require 'active_support'
require 'active_support/duration'
require 'active_support/values/time_zone'
require 'active_support/core_ext/time/calculations'
require 'active_support/core_ext/date/calculations'
require 'active_support/core_ext/date_time/calculations'
require 'date'

class Masamune::DataPlanRule
  attr_reader :pattern, :options

  def initialize(pattern, options = {})
    @pattern = pattern
    @options = options
    @matcher = unbind_pattern(pattern)
  end

  def ==(other)
    pattern == other.pattern &&
    options == other.options
  end

  def pattern
    @pattern
  end

  def matches?(input_path)
    matched_pattern = @matcher.match(input_path)
    matched_pattern.present? && matched_pattern[:rest].blank?
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

  def generate_via_unify_path(input_path, rule, &block)
    instance = unify_path(input_path, rule)

    stop_time = instance.start_time.advance(time_step => 1)
    begin
      yield instance
      instance = instance.next
    end while instance.start_time < stop_time
  end

  def tz
    ActiveSupport::TimeZone[@options.fetch(:tz, 'UTC')]
  end

  def time_step
    case @pattern
    when /%-?k/, /%-?H/
      :hours
    when /%-?d/
      :days
    when /%-?m/
      :months
    when /%-?Y/
      :years
    else
      raise "No time value for pattern #{@patter}"
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
    regexp.gsub!('*', '(?<glob>.*?)')
    regexp.gsub!(/$/, '(?<rest>.*?)\z')
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
