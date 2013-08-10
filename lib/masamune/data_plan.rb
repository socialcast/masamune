require 'active_support'
require 'active_support/core_ext/numeric/time'

class Masamune::DataPlan
  include Masamune::Accumulate

  def initialize
    @target_rules = Hash.new
    @source_rules = Hash.new
    @command_rules = Hash.new
    @targets = Hash.new { |set,rule| set[rule] = Masamune::DataPlanSet.new(@target_rules[rule]) }
    @sources = Hash.new { |set,rule| set[rule] = Masamune::DataPlanSet.new(@source_rules[rule]) }
  end

  def add_target_rule(rule, target, target_options = {})
    @target_rules[rule] = Masamune::DataPlanRule.new(self, rule, :target, target, target_options)
  end

  def add_source_rule(rule, source, source_options = {})
    @source_rules[rule] = Masamune::DataPlanRule.new(self, rule, :source, source, source_options)
  end

  def add_command_rule(rule, command, command_options = {})
    @command_rules[rule] = [command, command_options]
  end

  def rule_for_target(target)
    matches = @target_rules.select { |rule, matcher| matcher.matches?(target) }
    Masamune.logger.debug("No rule matches target #{target}") and return Masamune::DataPlanRule::TERMINAL if matches.empty?
    Masamune.logger.error("Multiple rules match target #{target}") if matches.length > 1
    matches.map(&:first).first
  end

  # TODO covert to DataPlanSet
  def targets_for_date_range(rule, start, stop, &block)
    target_template = @target_rules[rule]
    target_template.generate(start.to_time.utc, stop.to_time.utc) do |target_instance|
      yield target_instance
    end
  end
  method_accumulate :targets_for_date_range

  # TODO micro cache - clear on prepare phase
  def targets_for_source(rule, source)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    source_instance = source.is_a?(Masamune::DataPlanElem) ? source : source_template.bind_path(source)
    Masamune::DataPlanSet.new(target_template).tap do |set|
      source_template.generate_via_unify_path(source_instance.path, target_template) do |target|
        set.add target
      end
    end
  end

  # TODO micro cache - clear on prepare phase
  def sources_for_target(rule, target)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    target_instance = target.is_a?(Masamune::DataPlanElem) ? target : target_template.bind_path(target)
    Masamune::DataPlanSet.new(source_template).tap do |set|
      target_template.generate_via_unify_path(target_instance.path, source_template) do |source|
        set.add source
      end
    end
  end

  def prepare(rule, options = {})
    @targets[rule].merge options.fetch(:targets, [])
    @sources[rule].merge options.fetch(:sources, [])
  end

  def execute(rule, options = {})
    return if targets(rule).missing.empty?
    # TODO replace with source.rule.name
    sources(rule).missing.group_by { |source| rule_for_target(source.path) }.each do |derived_rule, sources|
      if derived_rule != Masamune::DataPlanRule::TERMINAL
        prepare(derived_rule, {targets: sources.map(&:path)})
        execute(derived_rule, options)
      end
    end

    command, command_options = @command_rules[rule]
    command.call(self, rule, options)
  end

  # TODO micro cache - clear on prepare phase
  def targets(rule)
    result = @sources[rule].map { |source| targets_for_source(rule, source) }.reduce(&:union)
    @targets[rule].union(result)
  end

  # TODO micro cache - clear on prepare phase
  def sources(rule)
    result = @targets[rule].map { |target| sources_for_target(rule, target) }.reduce(&:union)
    @sources[rule].union(result).adjacent
  end
end
