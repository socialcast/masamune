require 'active_support'
require 'active_support/core_ext/numeric/time'

class Masamune::DataPlan
  require 'masamune/actions/filesystem'
  include Masamune::Actions::Filesystem

  def initialize
    @targets = Hash.new
    @sources = Hash.new
    @matcher = Hash.new
    @commands = Hash.new
  end

  def add_target(rule, target, options = {})
    @targets[rule] = [target, options]
    @matcher[rule] = Masamune::Matcher.new(target)
  end

  def add_source(rule, source, options = {})
    @sources[rule] = [source, options]
  end

  def add_command(rule, command, options = {})
    @commands[rule] = [command, options]
  end

  def bind_date(date, template, tz_name = 'UTC')
    tz = ActiveSupport::TimeZone[tz_name]
    tz.utc_to_local(date).strftime(template)
  end

  def rule_for_target(target)
    @matcher.select { |rule, matcher| matcher.matches?(target) }.map(&:first).first
  end

  def targets(rule, start, stop)
    pattern, options, _, _ = @targets[rule]
    start_time, stop_time = start.to_time.utc, stop.to_time.utc
    step = self.class.rule_step(pattern)
    [].tap do |out|
      current_time = start_time
      while current_time <= stop_time do
        out << bind_date(current_time, pattern, options.fetch(:tz, 'UTC'))
        current_time += step
      end
    end.compact
  end

  def sources(rule, target)
    pattern, options = @sources[rule]
    if result = @matcher[rule].bind(target, pattern, options.fetch(:tz, 'UTC'))
      [].tap do |out|
        if options[:wildcard]
          fs.glob(result) do |file|
            out << file
          end
        else
          out << result
        end
      end.compact
    else
      []
    end
  end

  def analyze(rule, targets)
    matches, missing = [], Hash.new { |h,k| h[k] = [] }
    targets.each do |target|
      unless fs.exists?(target)
        sources = sources(rule, target)
        sources.each do |source|
          if fs.exists?(source)
            matches << source
          else
            dep = rule_for_target(source)
            missing[dep] << source
          end
        end
      end
    end
    [matches, missing]
  end

  def execute(rule, targets)
    matches, missing = analyze(rule, targets)

    missing.each do |dep, matches|
      execute(dep, matches)
    end

    matches, missing = analyze(rule, targets) if missing.any?

    command, options = @commands[rule]
    if matches.any?
      command.call(*matches)
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
end
