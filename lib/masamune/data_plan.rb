require 'active_support'
require 'active_support/core_ext/numeric/time'

class Masamune::DataPlan
  include Masamune::Accumulate

  def initialize
    @target_rules = Hash.new
    @source_rules = Hash.new
    @command_rules = Hash.new
    @targets = Hash.new { |h,k| h[k] = Masamune::DataPlanSet.new }
    @sources = Hash.new { |h,k| h[k] = Masamune::DataPlanSet.new }
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

  def sources_from_paths2(rule, *paths)
    Masamune::DataPlanSet.new(sources_from_paths(rule, paths))
  end

  def targets_from_paths(rule, *paths, &block)
    target_template = @target_rules[rule]
    paths.flatten.each do |path|
      yield target_template.bind_path(path)
    end
  end
  method_accumulate :targets_from_paths

  def targets_from_paths2(rule, *paths)
    Masamune::DataPlanSet.new(targets_from_paths(rule, paths))
  end

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

  def targets_for_source2(rule, source)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    Masamune::DataPlanSet.new.tap do |set|
      source_template.generate_via_unify_path(source.path, target_template) do |target|
        set.add(target)
      end
    end
  end

  def sources_for_target2(rule, target)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    Masamune::DataPlanSet.new.tap do |set|
      target_template.generate_via_unify_path(target.path, source_template) do |source|
        set.add(source)
      end
    end
  end

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
    @targets[rule].merge targets_from_paths2(rule, *options.fetch(:targets, []))
    @sources[rule].merge sources_from_paths2(rule, *options.fetch(:sources, []))
  end

  def targets(rule)
    result = @sources[rule].map { |source| targets_for_source2(rule, source) }.reduce(&:union)
    @targets[rule].union(result)
  end

  def sources(rule)
    result = @targets[rule].map { |target| sources_for_target2(rule, target) }.reduce(&:union)
    @sources[rule].union(result)
  end
end
