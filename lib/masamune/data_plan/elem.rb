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

class Masamune::DataPlan::Elem
  MISSING_MODIFIED_AT = Time.new(0)

  include Comparable

  attr_reader :rule, :options

  def initialize(rule, start_time, options = {})
    @rule = rule
    self.start_time = start_time
    @options = options
  end

  def input
    @input ||= start_time.strftime(strftime_format)
  end
  alias :path :input
  alias :table :input

  def partition
    input.split('_').last
  end
  alias :suffix :partition

  def exists?
    if rule.for_path?
      rule.engine.filesystem.exists?(path)
    elsif rule.for_table?
      rule.engine.postgres_helper.table_exists?(table)
    end
  end

  def complete?
    if rule.for_targets?
      sources.existing.map(&:start_date).uniq.length == sources.map(&:start_date).uniq.length
    end
  end

  def last_modified_at
    if rule.for_path?
      rule.engine.filesystem.stat(path).try(:mtime)
    elsif rule.for_table?
      rule.engine.postgres_helper.table_last_modified_at(table, @options)
    end || MISSING_MODIFIED_AT
  end

  def explode
    return Set.new(to_enum(__method__)) unless block_given?
    if rule.for_path? && rule.free?
      file_glob = path
      file_glob += '/' unless path.include?('*') || path.include?('.')
      file_glob += '*' unless path.include?('*')
      rule.engine.filesystem.glob(file_glob) do |new_path|
        yield rule.bind_input(new_path)
      end
    elsif rule.for_path? && rule.bound?
      yield self if exists?
    elsif rule.for_table?
      yield self if exists?
    end
  end

  def targets
    return Masamune::DataPlan::Set::EMPTY if @rule.for_targets?
    return Masamune::DataPlan::Set.new(rule.engine.get_target_rule(rule.name), to_enum(__method__)) unless block_given?
    rule.engine.targets_for_source(rule.name, self).each do |target|
      yield target
    end
  end

  def target
    targets.first
  end

  def sources
    return Masamune::DataPlan::Set::EMPTY if @rule.for_sources?
    return Masamune::DataPlan::Set.new(rule.engine.get_source_rule(rule.name), to_enum(__method__)) unless block_given?
    rule.engine.sources_for_target(rule.name, self).each do |source|
      yield source
    end
  end

  def source
    sources.first
  end

  def start_time
    @start_time.to_time.utc
  end

  def start_time=(start_time)
    @start_time =
    case start_time
    when Time
      rule.time_round(start_time.utc)
    when Date, DateTime
      rule.time_round(start_time.to_time.utc)
    end
  end

  def start_date
    @start_time.to_date
  end

  def stop_time
    start_time.advance(@rule.time_step => 1)
  end

  def stop_date
    stop_time.to_date
  end

  def glob
    @options[:glob]
  end

  def rest
    @options[:rest]
  end

  def next(i = 1)
    self.class.new(@rule, start_time.advance(@rule.time_step => +1*i), @options)
  end

  def prev(i = 1)
    self.class.new(@rule, start_time.advance(@rule.time_step => -1*i), @options)
  end

  def round(grain)
    self.class.new(@rule.round(grain), start_time, @options)
  end

  def ==(other)
    uniq_constraint == other.uniq_constraint
  end

  def eql?(other)
    self == other
  end

  def hash
    uniq_constraint.hash
  end

  # FIXME should consider stop_time for correctness
  def <=>(other)
    if start_time < other.start_time
      1
    elsif start_time > other.start_time
      -1
    elsif start_time == other.start_time
      0
    end
  end

  def inspect
    {rule: rule, input: input, start_date: start_time.to_s, stop_date: stop_time.to_s, :options => options}.to_s
  end

  protected

  def uniq_constraint
    [rule, options, rule.for_table? ? start_time : input]
  end

  private

  def strftime_format
    @strftime_format ||= begin
      format = @rule.strftime_format.dup
      format.sub!('*', glob || rest) if glob || rest
      format
    end
  end
end
