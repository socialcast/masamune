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

require 'singleton'

class Masamune::DataPlan::Builder
  include Singleton

  def build(namespaces, commands, sources, targets)
    Masamune::DataPlan::Engine.new.tap do |engine|
      sources_for, sources_anon = partition_by_for(sources)
      targets_for, targets_anon = partition_by_for(targets)

      commands.each do |name|
        command_name = "#{namespaces.shift}:#{name}"

        source_options = sources_for[name] || sources_anon.shift or next
        target_options = targets_for[name] || targets_anon.shift or next
        next if source_options[:skip] || target_options[:skip]

        engine.add_source_rule(command_name, source_options)
        engine.add_target_rule(command_name, target_options)

        engine.add_command_rule(command_name, thor_command_wrapper)
      end
    end
  end

  private

  def partition_by_for(annotations)
    with_for, anon = annotations.partition { |opts| opts.has_key?(:for) }
    decl = {}
    with_for.each do |opts|
      decl[opts[:for]] = opts.reject { |k,_| k == :for }
    end
    [decl, anon]
  end

  def thor_command_wrapper
    Proc.new do |engine, rule, _|
      engine.environment.parent.invoke(rule)
    end
  end
end
