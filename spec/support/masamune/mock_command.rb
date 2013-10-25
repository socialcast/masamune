module Masamune::MockCommand
  class CommandMatcher
    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    attr_accessor :pattern, :value

    def initialize(delegate, options = {})
      @delegate    = delegate
      self.pattern = options[:pattern]
      self.value   = options[:value]
    end

    def around_execute(&block)
      if @delegate.command_args.join(' ') =~ pattern
        value
      else
        @delegate.around_execute(&block)
      end
    end
  end

  def mock_success
    OpenStruct.new(:success? => true)
  end

  def mock_failure
    OpenStruct.new(:success? => false)
  end

  def mock_command(pattern, value)
    new_method = Masamune::Commands::Shell.method(:new)
    Masamune::Commands::Shell.stub(:new).and_return do |command, options|
      new_method.call(CommandMatcher.new(command, pattern: pattern, value: value), options || {})
    end
  end
end

RSpec.configure do |config|
  config.include Masamune::MockCommand
end
