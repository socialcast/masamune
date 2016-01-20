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

require 'delegate'

require 'masamune/actions/execute'

module Masamune::Commands
  class ElasticMapReduce < SimpleDelegator
    include Masamune::Actions::Execute

    DEFAULT_ATTRIBUTES =
    {
      :path       => 'elastic-mapreduce',
      :options    => [],
      :extra      => [],
      :jobflow    => nil,
      :input      => nil,
    }

    def initialize(delegate, attrs = {})
      super delegate
      @delegate = delegate
      DEFAULT_ATTRIBUTES.merge(configuration.elastic_mapreduce).merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    def interactive?
      if @delegate.respond_to?(:interactive?)
        @delegate.interactive?
      elsif @extra.any?
        true
      else
        @input == nil
      end
    end

    def stdin
      if @delegate.respond_to?(:stdin)
        @delegate.stdin
      elsif @input
        @stdin ||= StringIO.new(@input)
      end
    end

    def elastic_mapreduce_command
      args = []
      args << @path
      args << @options.map(&:to_a)
      args << ['--jobflow', @jobflow] if @jobflow
      args.flatten
    end

    def ssh_args
      args = []
      args << elastic_mapreduce_command
      args << '--ssh'
      args << 'exit'
      args.flatten
    end

    # Use elastic-mapreduce to translate jobflow into raw ssh command
    def ssh_command
      @ssh_command ||= begin
        result = nil
        execute(*ssh_args, fail_fast: true, safe: true) do |line|
          if line =~ /exit\Z/
            result = line.sub(/ exit\Z/, '').split(' ')
          else
            logger.error(line)
          end
        end
        result
      end
    end

    def command_args
      args = []
      args << (ssh_command? ? ssh_command : elastic_mapreduce_command)
      args << @extra
      args << @delegate.command_args if @delegate.respond_to?(:command_args)
      args.flatten
    end

    def handle_stdout(line, line_no)
      if line_no == 0 && line =~ /\Assh/
        @delegate.handle_stderr(line, line_no) if @delegate.respond_to?(:handle_stderr)
      else
        @delegate.handle_stdout(line, line_no) if @delegate.respond_to?(:handle_stdout)
      end
    end

    private

    def ssh_command?
      @delegate.respond_to?(:command_args) || @input.present?
    end
  end
end
