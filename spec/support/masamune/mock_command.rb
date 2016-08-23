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
require 'active_support/concern'

module Masamune::MockCommand
  extend ActiveSupport::Concern

  class CommandMatcher < SimpleDelegator
    def initialize(delegate)
      super delegate
      @delegate = delegate
    end

    class << self
      def add_pattern(pattern, value, io)
        @patterns ||= {}
        @patterns[pattern] = [value, io]
      end

      def patterns
        @patterns ||= {}
        @patterns
      end

      def reset!
        @patterns = {}
      end

      def match(pattern)
        # nop
      end
    end

    def command_env_and_args
      command_env = @delegate.respond_to?(:command_env) ? @delegate.command_env.map { |key, val| "#{key}=#{val}" } : []
      (command_env + @delegate.command_args).join(' ')
    end

    def around_execute(&block)
      self.class.patterns.each do |pattern, (value, io)|
        next unless command_env_and_args =~ pattern
        CommandMatcher.match(pattern)
        until io.eof?
          line = io.gets
          line_no ||= 0
          @delegate.handle_stdout(line.chomp, line_no) if @delegate.respond_to?(:handle_stdout)
          line_no += 1
        end
        return value.respond_to?(:call) ? value.call : value
      end

      if @delegate.respond_to?(:around_execute)
        @delegate.around_execute(&block)
      else
        yield
      end
    end
  end

  included do
    before do
      new_method = Masamune::Commands::Shell.method(:new)
      allow(Masamune::Commands::Shell).to receive(:new) do |command, options|
        new_method.call(CommandMatcher.new(command), options || {})
      end
    end

    after do
      CommandMatcher.reset!
    end
  end

  def mock_success
    OpenStruct.new(success?: true)
  end

  def mock_failure
    OpenStruct.new(success?: false)
  end

  def mock_command(pattern, value = nil, io = StringIO.new, &block)
    expect(CommandMatcher).to receive(:match).with(pattern)
    CommandMatcher.add_pattern(pattern, block_given? ? block.to_proc : value, io, &block)
  end
end

RSpec.configure do |config|
  config.include Masamune::MockCommand
end
