require 'active_support'
require 'active_support/core_ext/numeric/time'

class Masamune::DataPlan
  def initialize
    @rules = []
    @options = Hash.new { |h,k| h[k] = {} }
    @matches = Hash.new { |h,k| h[k] = SortedSet.new }
  end

  def add_rule(pattern, pattern_options, template, template_options, command, &block)
    @rules << [Masamune::Matcher.new(pattern), [template, template_options, command, block.to_proc]]
  end

  def resolve(start, stop)
    current = start.to_time.utc
    while current < stop.to_time.utc do
      @rules.each do |matcher, (template, options, command, filter)|
        target = matcher.bind_date(current)
        source = matcher.bind(target, template, options.fetch(:tz, 'UTC'))
        if !filter.call(target) && filter.call(source)
          @matches[command] << source
        end
      end
      current += 1.hour # TODO derive step from rule, allow override
    end
  end

  def matches
    @matches
  end
end
