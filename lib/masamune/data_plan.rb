require 'active_support'
require 'active_support/core_ext/numeric/time'

class Masamune::DataPlan
  include Masamune::Accumulate

  def initialize
    @targets = Hash.new
    @sources = Hash.new
    @commands = Hash.new
    @desired_sources = Hash.new { |h,k| h[k] = Set.new }
    @desired_targets = Hash.new { |h,k| h[k] = Set.new }
  end

  def add_target(rule, target, target_options = {})
    @targets[rule] = Masamune::DataPlanRule.new(target, target_options)
  end

  def add_source(rule, source, source_options = {})
    @sources[rule] = Masamune::DataPlanRule.new(source, source_options)
  end

  def add_command(rule, command, command_options = {})
    @commands[rule] = [command, command_options]
  end

  def rule_for_target(target)
    matches = @targets.select { |rule, matcher| matcher.matches?(target) }
    raise "No rule matches target #{target}" if matches.empty?
    raise "Multiple rules match target #{target}" if matches.length > 1
    matches.map(&:first).first
  end

  def sources_from_paths(rule, *paths, &block)
    source_template = @sources[rule]
    offered = Set.new
    paths.flatten.each do |path|
      instance = source_template.bind_path(path)
      source_template.adjacent_matches(instance) do |adjacent|
        next unless offered.add?(adjacent)
        yield adjacent
      end
    end
  end
  method_accumulate :sources_from_paths

  def targets_from_paths(rule, *paths, &block)
    target_template = @targets[rule]
    paths.flatten.each do |path|
      yield target_template.bind_path(path)
    end
  end
  method_accumulate :targets_from_paths

  def targets_for_date_range(rule, start, stop, &block)
    target_template = @targets[rule]
    target_template.generate(start.to_time.utc, stop.to_time.utc) do |target_instance|
      yield target_instance
    end
  end
  method_accumulate :targets_for_date_range

  def targets_for_source(rule, source_instance, &block)
    source_template = @sources[rule]
    target_template = @targets[rule]
    source_template.generate_via_unify_path(source_instance, target_template) do |target_instance|
      yield target_instance
    end
  end
  method_accumulate :targets_for_source

  def sources_for_target(rule, target_instance, &block)
    source_template = @sources[rule]
    target_template = @targets[rule]
    target_template.generate_via_unify_path(target_instance, source_template) do |source_instance|
      if source_instance.wildcard?
        Masamune.filesystem.glob(source_instance.path) do |source_path|
          sources_from_paths(rule, source_path).each do |source|
            yield source
          end
        end
      else
        yield source_instance
      end
    end
  end
  method_accumulate :sources_for_target

  # TODO declare partition for awareness
  # target requests/y/m/d/f, :table => requests, :partition => 'f'

  def prepare(rule, options = {})
    # TODO block size flag
    @desired_sources[rule] = options[:sources]
    @desired_targets[rule] = options[:targets] || targets_for_sources(rule, options[:sources]) || targets_for_date_range(rule, options[:start], options[:stop])
  end

  def execute(rule, options = {})
    existing_targets(rule).each do |source|
      # TODO force flag
      # if target for source is older delete
    end

    missing_sources(rule).group_by { |source| rule_for_target(source.path) }.each do |derived_rule, sources|
      execute(derived_rule, {sources: sources}.reverse_merge(options))
    end

    # TODO return if rule is terminal
    # TODO wait to acquire lock from lock manager
    @commands[rule].call(options)

    # TODO assert existing_targets == desired_targets
  end

  def missing_targets(rule)

  end

  def existing_targets(rule)
  end

  def missing_sources(rule)

  end

  def existing_sources(rule)
  end
end
