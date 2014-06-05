require 'active_support'
require 'active_support/core_ext/numeric/time'

require 'masamune/data_plan_set'

class Masamune::DataPlan
  MAX_DEPTH = 10

  include Masamune::HasContext
  include Masamune::Accumulate

  def initialize
    @target_rules = Hash.new
    @source_rules = Hash.new
    @command_rules = Hash.new
    @targets = Hash.new { |set,rule| set[rule] = Masamune::DataPlanSet.new(@target_rules[rule]) }
    @sources = Hash.new { |set,rule| set[rule] = Masamune::DataPlanSet.new(@source_rules[rule]) }
    @set_cache = Hash.new { |cache,level| cache[level] = Hash.new }
    @current_rule = nil
    @current_depth = 0
  end

  def add_target_rule(rule, target_options = {})
    @target_rules[rule] = Masamune::DataPlanRule.new(self, rule, :target, target_options)
  end

  def get_target_rule(rule)
    @target_rules[rule]
  end

  def add_source_rule(rule, source_options = {})
    @source_rules[rule] = Masamune::DataPlanRule.new(self, rule, :source, source_options)
  end

  def get_source_rule(rule)
    @source_rules[rule]
  end

  def add_command_rule(rule, command)
    @command_rules[rule] = command
  end

  # TODO use constructed reference instead
  def rule_for_target(target)
    target_matches = @target_rules.select { |rule, matcher| matcher.primary? && matcher.matches?(target) }
    source_matches = @source_rules.select { |rule, matcher| matcher.matches?(target) }

    if target_matches.empty?
      if source_matches.empty?
        raise "No rule matches target #{target}"
      else
        Masamune::DataPlanRule::TERMINAL
      end
    else
      logger.error("Multiple rules match target #{target}") if target_matches.length > 1
      target_matches.map(&:first).first
    end
  end

  # TODO convert to DataPlanSet
  def targets_for_date_range(rule, start, stop, &block)
    target_template = @target_rules[rule]
    target_template.generate(start.to_time.utc, stop.to_time.utc) do |target_instance|
      yield target_instance
    end if target_template
  end
  method_accumulate :targets_for_date_range

  def targets_for_source(rule, source, &block)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    source_instance = source.is_a?(Masamune::DataPlanElem) ? source : source_template.bind_input(source)
    source_template.generate_via_unify(source_instance, target_template) do |target|
      yield target
    end
  end
  method_accumulate :targets_for_source, lambda { |plan, rule, source| Masamune::DataPlanSet.new(plan.get_target_rule(rule)) }

  def sources_for_target(rule, target, &block)
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    target_instance = target.is_a?(Masamune::DataPlanElem) ? target : target_template.bind_input(target)
    target_template.generate_via_unify(target_instance, source_template) do |source|
      yield source
    end
  end
  method_accumulate :sources_for_target, lambda { |plan, rule, target| Masamune::DataPlanSet.new(plan.get_source_rule(rule)) }

  def targets(rule)
    @set_cache[:targets_for_rule][rule] ||= @targets[rule].union(@sources[rule].targets)
  end

  def sources(rule)
    @set_cache[:sources_for_rule][rule] ||= @sources[rule].union(@targets[rule].sources).adjacent
  end

  def prepare(rule, options = {})
    @targets[rule].merge options.fetch(:targets, [])
    @sources[rule].merge options.fetch(:sources, [])
  end

  def execute(rule, options = {})
    targets(rule).stale do |target|
      logger.warn("Detected stale target #{target.input}, removing")
      target.remove
    end
    context.clear!

    return if targets(rule).missing.empty?

    constrain_max_depth(rule) do
      sources(rule).missing.group_by { |source| rule_for_target(source.input) }.each do |derived_rule, sources|
        if derived_rule != Masamune::DataPlanRule::TERMINAL
          prepare(derived_rule, targets: sources.map(&:input))
          execute(derived_rule, options)
        end
      end
    end if options.fetch(:resolve, true)

    @current_rule = rule
    @command_rules[rule].call(self, rule, options)
    @set_cache.clear
    context.clear!
  ensure
    @current_rule = nil
  end

  def current_rule
    @current_rule
  end

  def constrain_max_depth(rule, &block)
    @current_depth += 1
    raise "Max depth of #{MAX_DEPTH} exceeded for rule '#{rule}'" if @current_depth > MAX_DEPTH
    yield
  ensure
    @current_depth -= 1
  end
end
