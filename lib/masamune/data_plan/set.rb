#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
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

require 'set'

class Masamune::DataPlan::Set < Set
  EMPTY = new

  attr_reader :rule

  def initialize(rule, enum = nil)
    @rule = rule
    super convert_enum(enum)
  end

  def union(enum = nil)
    super(convert_enum(enum) || EMPTY)
  end

  def add(elem = nil)
    super convert_elem(elem)
  end

  def include?(elem = nil)
    super convert_elem(elem)
  end

  def missing
    return self.class.new(rule, to_enum(__method__)) unless block_given?
    each do |elem|
      yield elem if elem.explode.none?
    end
  end

  def existing
    return self.class.new(rule, to_enum(__method__)) unless block_given?
    each do |elem|
      elem.explode.each do |new_elem|
        yield new_elem
      end
    end
  end

  def adjacent
    return self.class.new(rule, to_enum(__method__)) unless block_given?
    each do |elem|
      @rule.adjacent_matches(elem).each do |adj_elem|
        yield adj_elem
      end
    end
  end

  def stale
    return Masamune::DataPlan::Set::EMPTY if empty? || @rule.for_sources?
    return self.class.new(rule, to_enum(__method__)) unless block_given?
    each do |target|
      yield target if target.sources.existing.any? { |source| target_stale?(source, target) }
    end
  end

  def incomplete
    return Masamune::DataPlan::Set::EMPTY if empty? || @rule.for_sources?
    return self.class.new(rule, to_enum(__method__)) unless block_given?
    set = Set.new
    each do |target|
      yield target unless target.complete? || !set.add?(target)
    end
  end

  def actionable
    return Masamune::DataPlan::Set::EMPTY if empty? || @rule.for_sources?
    return self.class.new(rule, to_enum(__method__)) unless block_given?
    set = Set.new
    missing do |target|
      yield target if set.add?(target)
    end
    incomplete do |target|
      yield target if set.add?(target)
    end
    stale do |target|
      yield target if set.add?(target)
    end
  end

  def updateable
    return Masamune::DataPlan::Set::EMPTY if empty? || @rule.for_sources?
    return self.class.new(rule, to_enum(__method__)) unless block_given?
    set = Set.new
    actionable do |target|
      yield target if set.add?(target) && target.sources.existing.any?
    end
  end

  # TODO: detect & warn or correct if coarser grain set is incomplete
  def with_grain(grain)
    return self.class.new(rule.round(grain), to_enum(:with_grain, grain)) unless block_given?
    seen = Set.new
    each do |elem|
      granular_elem = elem.round(grain)
      yield granular_elem if seen.add?(granular_elem)
    end
  end

  def targets
    return Masamune::DataPlan::Set::EMPTY if empty? || @rule.for_targets?
    return self.class.new(first.targets.rule, to_enum(__method__)) unless block_given?
    each do |elem|
      elem.targets do |target|
        yield target
      end
    end
  end

  def sources
    return Masamune::DataPlan::Set::EMPTY if empty? || @rule.for_sources?
    return self.class.new(first.sources.rule, to_enum(__method__)) unless block_given?
    each do |elem|
      elem.sources do |source|
        yield source
      end
    end
  end

  private

  def convert_elem(elem)
    case elem
    when nil
    when Masamune::DataPlan::Elem
      elem
    when String
      @rule.bind_input(elem)
    else
      raise "Unhandled elem class #{elem.class}"
    end
  end

  def convert_enum(enum)
    case enum
    when nil
    when Array
      enum.flatten.uniq
    when Set, self.class, Enumerator
      enum
    when String
      [enum]
    else
      raise "Unhandled enum class #{enum.class}"
    end
  end

  def target_stale?(source, target)
    target.last_modified_at != Masamune::DataPlan::Elem::MISSING_MODIFIED_AT &&
    source.last_modified_at != Masamune::DataPlan::Elem::MISSING_MODIFIED_AT &&
    source.last_modified_at > target.last_modified_at
  end
end
