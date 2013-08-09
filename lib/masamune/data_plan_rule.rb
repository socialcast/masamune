require 'active_support'
require 'active_support/duration'
require 'active_support/values/time_zone'
require 'active_support/core_ext/time/calculations'
require 'active_support/core_ext/date/calculations'
require 'active_support/core_ext/date_time/calculations'
require 'date'

class Masamune::DataPlanRule
  TERMINAL = true

  include Masamune::Accumulate

  attr_reader :type, :pattern, :options

  def initialize(plan, name, type, pattern, options = {})
    @plan    = plan
    @name    = name
    @type    = type
    @pattern = pattern
    @options = options
  end

  def ==(other)
    pattern == other.pattern &&
    options == other.options
  end

  def plan
    @plan
  end

  def name
    @name
  end

  def type
    @type
  end

  def pattern
    @pattern.respond_to?(:call) ? @pattern.call : @pattern
  end

  def matches?(input_path)
    matched_pattern = matcher.match(input_path)
    matched_pattern.present? && matched_pattern[:rest].blank?
  end

  def bind_date(input_date)
    output_date = tz.utc_to_local(input_date)
    Masamune::DataPlanElem.new(self, output_date, @options)
  end

  def bind_path(input_path)
    matched_pattern = matcher.match(input_path)
    raise "Cannot bind_path #{input_path} to #{pattern}" unless matched_pattern
    output_date = matched_date(matched_pattern)
    Masamune::DataPlanElem.new(self, output_date, @options.merge(matched_extra(matched_pattern)))
  end

  def unify_path(input_path, rule)
    matched_pattern = matcher.match(input_path)
    raise "Cannot unify_path #{input_path} with #{rule.pattern}, does not match #{pattern}" unless matched_pattern
    output_date = matched_date(matched_pattern)
    rule.bind_date(output_date)
  end

  def generate(start_time, stop_time, &block)
    instance = bind_date(start_time)
    begin
      yield instance
      instance = instance.next
    end while instance.start_time <= stop_time
  end
  method_accumulate :generate

  def generate_via_unify_path(input_path, rule, &block)
    instance = unify_path(input_path, rule)

    stop_time = instance.start_time.advance(time_step => 1)
    begin
      yield instance
      instance = instance.next
    end while instance.start_time < stop_time
  end
  method_accumulate :generate_via_unify_path

  def tz
    ActiveSupport::TimeZone[@options.fetch(:tz, 'UTC')]
  end

  def time_step
    case pattern
    when /%-?k/, /%-?H/
      :hours
    when /%-?d/
      :days
    when /%-?m/
      :months
    when /%-?Y/
      :years
    else
      raise "No time value for pattern #{pattern}"
    end
  end

  def adjacent_matches(instance)
    window = @options[:window] || 0
    (-window .. -1).each do |i|
      yield instance.prev(i.abs)
    end
    yield instance
    (1 .. window).each do |i|
      yield instance.next(i)
    end
  end
  method_accumulate :adjacent_matches

  private

  def matcher
    @matcher ||= begin
      regexp = pattern.dup
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
  end

  def matched_hash(matched_pattern, *matched_keys)
    matched_attrs = matched_keys.select { |x| matched_pattern.names.map(&:to_sym).include?(x) }
    Hash[matched_attrs.map { |x| [x,matched_pattern[x]] }]
  end

  def matched_date(matched_pattern)
    DateTime.new(*matched_hash(matched_pattern, :year, :month, :day, :hour).values.map(&:to_i))
  end

  def matched_extra(matched_pattern)
    matched_hash(matched_pattern, :glob)
  end
end
