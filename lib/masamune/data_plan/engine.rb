#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'active_support'
require 'active_support/core_ext/numeric/time'

class Masamune::DataPlan::Engine
  MAX_DEPTH = 10

  include Masamune::HasEnvironment

  def initialize
    @target_rules = Hash.new
    @source_rules = Hash.new
    @command_rules = Hash.new
    @targets = Hash.new { |set,rule| set[rule] = Masamune::DataPlan::Set.new(@target_rules[rule]) }
    @sources = Hash.new { |set,rule| set[rule] = Masamune::DataPlan::Set.new(@source_rules[rule]) }
    @set_cache = Hash.new { |cache,level| cache[level] = Hash.new }
    @current_depth = 0
  end

  def filesystem
    @filesystem ||= Masamune::CachedFilesystem.new(environment.filesystem)
  end

  def add_target_rule(rule, target_options = {})
    @target_rules[rule] = Masamune::DataPlan::Rule.new(self, rule, :target, target_options)
  end

  def get_target_rule(rule)
    @target_rules[rule]
  end

  def add_source_rule(rule, source_options = {})
    @source_rules[rule] = Masamune::DataPlan::Rule.new(self, rule, :source, source_options)
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
        Masamune::DataPlan::Rule::TERMINAL
      end
    else
      logger.error("Multiple rules match target #{target}") if target_matches.length > 1
      target_matches.map(&:first).first
    end
  end

  def targets_for_date_range(rule, start, stop)
    return Masamune::DataPlan::Set.new(get_target_rule(rule), to_enum(:targets_for_date_range, rule, start, stop)) unless block_given?
    target_template = @target_rules[rule]
    return unless target_template
    target_template.generate(start.to_time.utc, stop.to_time.utc).each do |target|
      yield target
    end
  end

  def targets_for_source(rule, source)
    return Masamune::DataPlan::Set.new(get_target_rule(rule), to_enum(:targets_for_source, rule, source)) unless block_given?
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    source_instance = source_template.bind_input(source)
    source_template.generate_via_unify(source_instance, target_template).each do |target|
      yield target
    end
  end

  def sources_for_target(rule, target)
    return Masamune::DataPlan::Set.new(get_source_rule(rule), to_enum(:sources_for_target, rule, target)) unless block_given?
    source_template = @source_rules[rule]
    target_template = @target_rules[rule]
    target_instance = target_template.bind_input(target)
    target_template.generate_via_unify(target_instance, source_template).each do |source|
      yield source
    end
  end

  def targets(rule)
    @set_cache[:targets_for_rule][rule] ||= @targets[rule].union(@sources[rule].targets)
  end

  def sources(rule)
    @set_cache[:sources_for_rule][rule] ||= @sources[rule].union(@targets[rule].sources).adjacent
  end

  def prepare(rule, options = {})
    @targets[rule].merge options.fetch(:targets, [])
    @sources[rule].merge options.fetch(:sources, [])

    constrain_max_depth(rule) do
      sources(rule).group_by { |source| rule_for_target(source.input) }.each do |derived_rule, sources|
        prepare(derived_rule, targets: sources.map(&:input)) if derived_rule != Masamune::DataPlan::Rule::TERMINAL
      end
    end if options.fetch(:resolve, true)
    clear!
  end

  def execute(rule, options = {})
    return if targets(rule).actionable.empty?

    constrain_max_depth(rule) do
      sources(rule).group_by { |source| rule_for_target(source.input) }.each do |derived_rule, sources|
        execute(derived_rule, options) if derived_rule != Masamune::DataPlan::Rule::TERMINAL
      end
    end if options.fetch(:resolve, true)

    @command_rules[rule].call(self, rule, options)
    clear!
  end

  def executing?
    @current_depth > 0
  end

  def constrain_max_depth(rule)
    @current_depth += 1
    raise "Max depth of #{MAX_DEPTH} exceeded for rule '#{rule}'" if @current_depth > MAX_DEPTH
    yield
  ensure
    @current_depth -= 1
  end

  def clear!
    @set_cache.clear
    filesystem.clear!
    environment.postgres_helper.clear!
  end
end
