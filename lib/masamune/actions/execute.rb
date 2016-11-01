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

module Masamune::Actions
  module Execute
    def execute(*args)
      opts = args.last.is_a?(Hash) ? args.pop : {}
      opts = opts.to_hash.symbolize_keys

      klass = Class.new do
        include Masamune::HasEnvironment

        def initialize(delegate)
          self.environment = delegate
        end
      end

      klass.class_eval do
        define_method(:command_args) do
          args
        end

        define_method(:command_env) do
          opts[:env] || {}
        end
      end

      if opts[:input]
        klass.class_eval do
          define_method(:stdin) do
            @stdin ||= StringIO.new(opts[:input])
          end
        end
      end

      if block_given?
        klass.class_eval do
          define_method(:handle_stdout) do |line, line_no|
            yield(line, line_no)
          end
        end
      end

      command = klass.new(self)
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, { fail_fast: false }.merge(opts))
      opts.fetch(:interactive, false) ? command.replace(opts) : command.execute
    end
  end
end
