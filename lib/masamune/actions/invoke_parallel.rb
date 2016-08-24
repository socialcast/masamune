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

require 'parallel'

require 'masamune/actions/execute'

module Masamune::Actions
  module InvokeParallel
    extend ActiveSupport::Concern

    include Masamune::Actions::Execute

    included do |base|
      base.class_option :max_tasks, aliases: '-p', type: :numeric, desc: 'Maximum number of tasks to execute in parallel', default: Parallel.processor_count
    end

    def invoke_parallel(*task_group)
      per_task_opts = task_group.last.is_a?(Array) ? task_group.pop.dup : [{}]
      all_task_opts = task_group.last.is_a?(Hash) ? task_group.pop.dup : {}
      task_group_by_task_opts = task_group.product(per_task_opts)
      max_tasks = [all_task_opts.delete(:max_tasks), task_group_by_task_opts.count].min
      console("Setting max_tasks to #{max_tasks}")
      bail_fast task_group, all_task_opts if all_task_opts[:version]
      Parallel.each(task_group_by_task_opts, in_processes: max_tasks) do |task_name, task_opts|
        task_env = task_opts[:env] || {}
        begin
          execute(thor_wrapper, task_name, *task_args(all_task_opts.merge(task_opts)), interactive: true, detach: false, env: task_env)
        rescue SystemExit # rubocop:disable Lint/HandleExceptions
        end
      end
    end

    private

    def thor_wrapper
      'thor'
    end

    def bail_fast(task_group, opts = {})
      task_name = task_group.first
      task_env = task_opts[:env] || {}
      execute($PROGRAM_NAME, task_name, *task_args(opts), env: task_env)
      exit
    end

    def task_args(opts = {})
      opts.except(:env).map do |k, v|
        case v
        when true
          "--#{k.to_s.tr('_', '-')}"
        when false
        else
          ["--#{k.to_s.tr('_', '-')}", v]
        end
      end.flatten.compact
    end
  end
end
