require 'active_support'
require 'active_support/duration'
require 'active_support/values/time_zone'
require 'date'

class Masamune::Matcher
  def initialize(pattern, options = {})
    @pattern = pattern
    @options = options
    @matcher = unbind_pattern(pattern)
  end

  def matches?(input)
    @matcher.match(input) != nil
  end

  def bind(input, template, tz_name = 'UTC')
    if matched_pattern = @matcher.match(input)
      tz = ActiveSupport::TimeZone[tz_name]
      tz.utc_to_local(matched_date(matched_pattern)).strftime(template)
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
    Regexp.compile(regexp)
  end

  def matched_date(matched_pattern)
    matched_attrs = [:year, :month, :day, :hour].select { |x| matched_pattern.names.map(&:to_sym).include?(x) }
    DateTime.new(*matched_attrs.map { |x| matched_pattern[x].to_i })
  end
end
