require 'active_support'
require 'active_support/core_ext/numeric/time'

class Masamune::DataPlan
  require 'masamune/actions/filesystem'
  include Masamune::Actions::Filesystem

  def initialize
    @rules = []
    @options = Hash.new { |h,k| h[k] = {} }
    @matches = Hash.new { |h,k| h[k] = SortedSet.new }
  end

  def add_rule(pattern, pattern_options, template, template_options, command)
    @rules << [pattern, pattern_options, template, template_options, command]
  end

  def bind_date(date, template, tz_name = 'UTC')
    tz = ActiveSupport::TimeZone[tz_name]
    tz.utc_to_local(date).strftime(template)
  end

  def resolve(start, stop)
    start_time, stop_time = start.to_time.utc, stop.to_time.utc
    current_time = start_time
    while current_time < stop_time do
      @rules.each do |pattern, pattern_options, template, template_options, command, filter|
        target = bind_date(current_time, pattern, pattern_options.fetch(:tz, 'UTC'))
        source = bind_date(current_time, template, template_options.fetch(:tz, 'UTC'))
        if !fs.exists?(target)
          fs.glob(source) do |source_file|
            if fs.exists?(source_file)
              @matches[command] << source_file
            end
          end
        end
      end
      current_time += 1.hour # TODO derive step from rule, allow override
    end
  end

  def matches
    @matches
  end
end
