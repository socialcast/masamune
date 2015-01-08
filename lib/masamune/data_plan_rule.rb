require 'active_support'
require 'active_support/duration'
require 'active_support/values/time_zone'
require 'active_support/core_ext/time/calculations'
require 'active_support/core_ext/date/calculations'
require 'active_support/core_ext/date_time/calculations'
require 'date'

class Masamune::DataPlanRule
  TERMINAL = nil

  include Masamune::Accumulate

  attr_reader :plan, :name, :type, :options

  def initialize(plan, name, type, options = {})
    @plan    = plan
    @name    = name
    @type    = type
    @options = options
  end

  def for_targets?
    @type == :target
  end

  def for_sources?
    @type == :source
  end

  def for_path?
    @options.key?(:path)
  end

  def for_table?
    @options.key?(:table)
  end

  def for_hive_table?
    @options.key?(:hive_table) && @options.key?(:hive_partition)
  end

  def for_table_with_partition?
    @options.key?(:table) && @options.key?(:partition)
  end

  def path
    @options[:path]
  end

  def table
    @options[:table]
  end

  def partition
    @options[:partition]
  end

  def hive_table
    @options[:hive_table]
  end

  def hive_partition
    @options[:hive_partition]
  end

  def ==(other)
    plan    == other.plan &&
    name    == other.name &&
    type    == other.type &&
    pattern == other.pattern &&
    options == other.options
  end

  def eql?(other)
    self == other
  end

  def hash
    [plan, name, type, pattern, options].hash
  end

  def pattern
    @pattern ||= begin
      if for_path?
        path.respond_to?(:call) ? path.call(plan.filesystem) : path
      elsif for_table_with_partition?
        [table , partition].join('_')
      elsif for_table?
        table
      end.to_s
    end
  end

  def primary?
    @options.fetch(:primary, true)
  end

  def matches?(input)
    matched_pattern = match_data_hash(matcher.match(input))
    matched_pattern.present? && matched_pattern[:rest].blank?
  end

  def bind_date(input_date)
    output_date = tz.utc_to_local(input_date)
    Masamune::DataPlanElem.new(self, output_date, options_for_elem)
  end

  def bind_input(input)
    matched_pattern = match_data_hash(matcher.match(input))
    raise "Cannot bind_input #{input} to #{pattern}" unless matched_pattern
    output_date = matched_date(matched_pattern)
    Masamune::DataPlanElem.new(self, output_date, options_for_elem.merge(matched_extra(matched_pattern)))
  end

  def unify(elem, rule)
    rule.bind_date(elem.start_time)
  end

  def generate(start_time, stop_time, &block)
    instance = bind_date(start_time)

    begin
      yield instance
      instance = instance.next
    end while instance.start_time <= stop_time
  end
  method_accumulate :generate

  def generate_via_unify(elem, rule, &block)
    instance = unify(elem, rule)

    stop_time = instance.start_time.advance(time_step => 1)
    begin
      yield instance
      instance = instance.next
    end while instance.start_time < stop_time
  end
  method_accumulate :generate_via_unify

  def tz
    ActiveSupport::TimeZone[@options.fetch(:tz, 'UTC')]
  end

  def time_step
    @time_step ||=
    case pattern
    when /%s/
      :hours
    when /%H-s/
      :hours
    when /%d-s/
      :days
    when /%m-s/
      :months
    when /%Y-s/
      :years
    when /%-?k/, /%-?H/
      :hours
    when /%-?d/
      :days
    when /%-?m/
      :months
    when /%-?Y/
      :years
    else
      :hours
    end
  end

  def time_round(time)
    case time_step
    when :hours
      DateTime.civil(time.year, time.month, time.day, time.hour)
    when :days
      DateTime.civil(time.year, time.month, time.day)
    when :months
      DateTime.civil(time.year, time.month)
    when :years
      DateTime.civil(time.year)
    else
      time
    end
  end

  def window
    @options[:window] || 0
  end

  def adjacent_matches(instance)
    (-window .. -1).each do |i|
      yield instance.prev(i.abs)
    end
    yield instance
    (1 .. window).each do |i|
      yield instance.next(i)
    end
  end
  method_accumulate :adjacent_matches

  def inspect
    {type: type, pattern: pattern, options: options}.to_s
  end

  def strftime_format
    @strftime_format ||=
    pattern.dup.tap do |format|
      format.gsub!('%H-s', '%s')
      format.gsub!('%d-s', '%s')
      format.gsub!('%m-s', '%s')
      format.gsub!('%Y-s', '%s')
    end
  end

  def round(grain)
    pattern_parts = pattern.split('/')
    part_index = pattern_parts.find_index { |part| part =~ time_step_to_format(grain) }
    raise "cannot round to :#{grain} for #{pattern}" unless part_index
    new_pattern = pattern_parts[0..part_index].join('/')
    self.class.new(plan, name, type, options.merge(path: new_pattern))
  end

  private

  def time_step_to_format(step)
    case step
    when :hour, :hours
      /%-?[H|k]/
    when :day, :days
      /%-?d/
    when :month, :months
      /%-?m/
    when :year, :years
      /%Y/
    end
  end

  def matcher
    @matcher ||= begin
      regexp = pattern.dup
      regexp.gsub!(/%([YmdH]-)?s/, '(?<timestamp>\d{10})')
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

  def match_data_hash(match_data = nil)
    return unless match_data.present?
    Hash.new.tap do |hash|
      match_data.names.map(&:to_sym).each do |key|
        hash[key] = match_data[key.to_sym]
      end
    end
  end

  def matched_date(matched_data)
    if timestamp = matched_data[:timestamp]
      Time.at(timestamp.to_i).to_datetime
    else
      DateTime.new(*matched_data.values_at(:year, :month, :day, :hour).compact.map(&:to_i))
    end
  end

  def matched_extra(matched_data)
    return {} unless matched_data.has_key?(:glob)
    {glob: matched_data[:glob]}.reject { |_,v| v == '*' }
  end

  def options_for_elem
    @options.reject { |k,_| [:path, :table].include?(k) }
  end
end
