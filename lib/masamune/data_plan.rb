require 'active_support'
require 'active_support/core_ext/numeric/time'

class Masamune::DataPlan
  require 'masamune/actions/filesystem'
  include Masamune::Actions::Filesystem

  def initialize
    @rules = Hash.new
    @options = Hash.new { |h,k| h[k] = {} }
    @matches = Hash.new { |h,k| h[k] = SortedSet.new }
  end

  def add_rule(pattern, pattern_options, template, template_options, command)
    @rules[command] = [pattern, pattern_options, template, template_options]
  end

  def bind_date(date, template, tz_name = 'UTC')
    tz = ActiveSupport::TimeZone[tz_name]
    tz.utc_to_local(date).strftime(template)
  end

  def resolve(start, stop, command)
    pattern, pattern_options, template, template_options = @rules[command]
    start_time, stop_time = start.to_time.utc, stop.to_time.utc
    step = [self.class.rule_step(start_time), self.class.rule_step(stop_time)].min
    current_time = start_time
    while current_time <= stop_time do
      target = bind_date(current_time, pattern, pattern_options.fetch(:tz, 'UTC'))
      source = bind_date(current_time, template, template_options.fetch(:tz, 'UTC'))
      if !fs.exists?(target)
        fs.glob(source) do |source_file|
          if fs.exists?(source_file)
            # TODO enumerate matches
            @matches[command] << source_file
          end
        end
      end
      current_time += step
    end
  end

  def self.rule_step(pattern)
    case pattern
    when /%k/, /%H/
      1.hour.to_i
    when /%d/
      1.day.to_i
    else
      1.day.to_i
    end
  end

  def matches
    @matches
  end
end
