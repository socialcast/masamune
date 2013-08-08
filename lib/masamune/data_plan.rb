require 'active_support'
require 'active_support/core_ext/numeric/time'

class Masamune::DataPlan
  include Masamune::Accumulate

  def initialize
    @target_rules = Hash.new
    @source_rules = Hash.new
    @command_rules = Hash.new
    @desired_sources = Hash.new { |h,k| h[k] = Set.new }
    @desired_targets = Hash.new { |h,k| h[k] = Set.new }
  end

  def add_target_rule(rule, target, target_options = {})
    @target_rules[rule] = Masamune::DataPlanRule.new(self, :target, target, target_options)
  end

  def add_source_rule(rule, source, source_options = {})
    @source_rules[rule] = Masamune::DataPlanRule.new(self, :source, source, source_options)
  end

  def add_command_rule(rule, command, command_options = {})
    @command_rules[rule] = [command, command_options]
  end

  def rule_for_target(target)
    matches = @target_rules.select { |rule, matcher| matcher.matches?(target) }
    raise "No rule matches target #{target}" if matches.empty?
    raise "Multiple rules match target #{target}" if matches.length > 1
    matches.map(&:first).first
  end

  def sources_from_paths(rule, *paths, &block)
    source_template = @source_rules[rule]
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
    target_template = @target_rules[rule]
    paths.flatten.each do |path|
      yield target_template.bind_path(path)
    end
  end
  method_accumulate :targets_from_paths

  def targets_for_date_range(rule, start, stop, &block)
    target_template = @target_rules[rule]
    target_template.generate(start.to_time.utc, stop.to_time.utc) do |target_instance|
      yield target_instance
    end
  end
  method_accumulate :targets_for_date_range

  def targets_for_source(rule, source_instance, &block)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    source_template.generate_via_unify_path(source_instance, target_template) do |target_instance|
      yield target_instance
    end
  end
  method_accumulate :targets_for_source

  def sources_for_target(rule, target_instance, &block)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
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

  def targets_for_source2(rule, source, &block)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    source_template.generate_via_unify_path(source.path, target_template) do |target|
      yield target
    end
  end
  method_accumulate :targets_for_source2

  def sources_for_target2(rule, target, &block)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    target_template.generate_via_unify_path(target.path, source_template) do |source|
      yield source
    end
  end
  method_accumulate :sources_for_target2

  def analyze(rule, targets)
    matches, missing = Set.new, Hash.new { |h,k| h[k] = Set.new }
    targets.each do |target|
      unless Masamune.filesystem.exists?(target)
        sources_for_target(rule, target) do |source|
          if Masamune.filesystem.exists?(source.path)
            matches << source
          else
            next if source.terminal?
            rule_dep = rule_for_target(source.path)
            missing[rule_dep] << source
          end
        end
      end
    end
    [matches, missing]
  end

  def resolve(rule, targets, runtime_options = {})
    matches, missing = analyze(rule, targets)

    missing.each do |rule_dep, missing_sources|
      resolve(rule_dep, missing_sources.map(&:path), runtime_options)
    end

    matches, missing = analyze(rule, targets) if missing.any?

    command, command_options = @command_rules[rule]
    if matches.any?
      command.call(matches.map(&:path), runtime_options)
      true
    else
      false
    end
  end

  def prepare(rule, options = {})
    @desired_targets[rule].merge(targets_from_paths(rule, *options.fetch(:targets, [])))
    @desired_sources[rule].merge(sources_from_paths(rule, *options.fetch(:sources, [])))
  end

  def desired_targets(rule)
    @desired_targets[rule].union(@desired_sources[rule].map { |source| targets_for_source2(rule, source) }.flatten)
  end

  def missing_targets(rule, &block)
    desired_targets(rule).each do |target|
      yield target unless Masamune.filesystem.exists?(target.path)
    end
  end
  method_accumulate :missing_targets

  def existing_targets(rule, &block)
    desired_targets(rule).each do |target|
      yield target if Masamune.filesystem.exists?(target.path)
    end
  end
  method_accumulate :existing_targets

  def desired_sources(rule)
    @desired_sources[rule].union(@desired_targets[rule].map { |target| sources_for_target2(rule, target) }.flatten)
  end

  def missing_sources(rule, &block)
    desired_sources(rule).each do |source|
      yield source if Masamune.filesystem.glob(source.path).empty?
    end
  end
  method_accumulate :missing_sources

  def existing_sources(rule, &block)
    desired_sources(rule).each do |source|
      Masamune.filesystem.glob(source.path) do |path|
        sources_from_paths(rule, path).each do |source|
          yield source
        end
      end
    end
  end
  method_accumulate :existing_sources
end
