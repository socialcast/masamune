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
      attr_accessor :patterns
      def add_pattern(pattern, value)
        self.patterns ||= {}
        self.patterns[pattern] = value
      end

      def reset!
        self.patterns = {}
      end
    end

    def around_execute(&block)
      self.class.patterns.each do |pattern, value|
        if @delegate.command_args.join(' ') =~ pattern
          return value
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
      Masamune::Commands::Shell.stub(:new).and_return do |command, options|
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

  def mock_command(pattern, value)
    CommandMatcher.add_pattern(pattern, value)
  end
end

RSpec.configure do |config|
  config.include Masamune::MockCommand
end
