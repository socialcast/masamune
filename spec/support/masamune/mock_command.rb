require 'active_support/concern'

module Masamune::MockCommand
  extend ActiveSupport::Concern

  class CommandMatcher
    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    def initialize(delegate)
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
    end

    def around_execute(&block)
      self.class.patterns.each do |pattern, (value, io)|
        if @delegate.command_args.join(' ') =~ pattern
          while line = io.gets
            line_no ||= 0
            @delegate.handle_stdout(line.chomp, line_no) if @delegate.respond_to?(:handle_stdout)
            line_no += 1
          end
          return value.respond_to?(:call) ? value.call : value
        end
      end

      if @delegate.respond_to?(:around_execute)
        @delegate.around_execute(&block)
      else
        block.call
      end
    end
  end

  included do |base|
    base.before do
      new_method = Masamune::Commands::Shell.method(:new)
      allow(Masamune::Commands::Shell).to receive(:new) do |command, options|
        new_method.call(CommandMatcher.new(command), options || {})
      end
    end

    base.after do
      CommandMatcher.reset!
    end
  end

  def mock_success
    OpenStruct.new(:success? => true)
  end

  def mock_failure
    OpenStruct.new(:success? => false)
  end

  def mock_command(pattern, value = nil, io = StringIO.new, &block)
    CommandMatcher.add_pattern(pattern, block_given? ? block.to_proc : value, io, &block)
  end
end

RSpec.configure do |config|
  config.include Masamune::MockCommand
end
