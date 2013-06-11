require 'active_support'
require 'active_support/core_ext/numeric/time'

# TODO all operations should be on DataPlan::Elem, not String paths
class Masamune::DataPlan
  def initialize
    @targets = Hash.new
    @sources = Hash.new
    @commands = Hash.new
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

  def sources_from_paths(rule, *paths)
    source_template = @sources[rule]
    [].tap do |sources|
      paths.flatten.each do |path|
        sources << source_template.bind_path(path)
      end
    end
  end

  def targets_from_paths(rule, *paths)
    target_template = @targets[rule]
    [].tap do |targets|
      paths.flatten.each do |path|
        targets << target_template.bind_path(path)
      end
    end
  end

  def targets_for_date_range(rule, start, stop, &block)
    if block_given?
      targets_for_date_range_block(rule, start, stop, &block)
    else
      [].tap do |targets|
        targets_for_date_range_block(rule, start, stop) do |target|
          targets << target
        end
      end
    end
  end

  def targets_for_date_range_block(rule, start, stop, &block)
    target_template = @targets[rule]
    target_template.generate(start.to_time.utc, stop.to_time.utc) do |target_instance|
      yield target_instance
    end
  end

  def targets_for_source(rule, source_instance, &block)
    source_template = @sources[rule]
    target_template = @targets[rule]
    if block_given?
      source_template.generate_via_unify_path(source_instance, target_template) do |target_instance|
        yield target_instance
      end
    else
      [].tap do |out|
        targets_for_source(rule, source_instance) { |target_instance| out << target_instance }
      end
    end
  end

  def sources_for_target(rule, target_instance, &block)
    source_template = @sources[rule]
    target_template = @targets[rule]
    if block_given?
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
    else
      [].tap do |out|
        sources_for_target(rule, target_instance) { |source_instance| out << source_instance }
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

    command, command_options = @commands[rule]
    if matches.any?
      command.call(matches.map(&:path), runtime_options)
      true
    else
      false
    end
  end
end
