#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'active_support'
require 'active_support/duration'
require 'active_support/values/time_zone'
require 'active_support/core_ext/time/calculations'
require 'active_support/core_ext/date/calculations'
require 'active_support/core_ext/date_time/calculations'
require 'date'

class Masamune::DataPlan::Rule
  TERMINAL = nil

  attr_reader :engine, :name, :type, :options

  def initialize(engine, name, type, options = {})
    @engine  = engine
    @name    = name
    @type    = type
    @options = options
  end

  def prepare
    pattern
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

  def ==(other)
    engine  == other.engine &&
    name    == other.name &&
    type    == other.type &&
    pattern == other.pattern &&
    options == other.options
  end

  def eql?(other)
    self == other
  end

  def hash
    [engine, name, type, pattern, options].hash
  end

  def pattern
    @pattern ||= begin
      if for_path?
        path.respond_to?(:call) ? path.call(engine.filesystem) : path
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

  def bind_date_or_time(input = nil)
    input_time =
    case input
    when Time, DateTime
      input
    when Date
      input.to_time
    else
      raise ArgumentError, "Cannot bind_date_or_time with type #{input.class}"
    end
    output_time = tz.utc_to_local(input_time)
    Masamune::DataPlan::Elem.new(self, output_time, options_for_elem)
  end

  def bind_input(input)
    return input if input.is_a?(Masamune::DataPlan::Elem)
    matched_pattern = match_data_hash(matcher.match(input))
    raise "Cannot bind_input #{input} to #{pattern}" unless matched_pattern
    output_date = matched_date(matched_pattern)
    Masamune::DataPlan::Elem.new(self, output_date, options_for_elem.merge(matched_extra(matched_pattern)))
  end

  def unify(elem, rule)
    rule.bind_date_or_time(elem.start_time)
  end

  def generate(start_time, stop_time)
    return Set.new(to_enum(:generate, start_time, stop_time)) unless block_given?
    instance = bind_date_or_time(start_time)

    begin
      yield instance
      instance = instance.next
    end while instance.start_time <= stop_time
  end

  def generate_via_unify(elem, rule)
    return Set.new(to_enum(:generate_via_unify, elem, rule)) unless block_given?
    instance = unify(elem, rule)

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
    return Set.new(to_enum(:adjacent_matches, instance)) unless block_given?
    (-window .. -1).each do |i|
      yield instance.prev(i.abs)
    end
    yield instance
    (1 .. window).each do |i|
      yield instance.next(i)
    end
  end

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
    self.class.new(engine, name, type, options.merge(path: new_pattern))
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
