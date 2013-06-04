module Masamune::Actions
  module Execute
    require 'masamune/commands/shell'

    def execute(*args, &block)
      opts = args.last.is_a?(Hash) ? args.pop : {}

      klass = Class.new
      klass.class_eval do
        define_method(:command_args) do
          args
        end
      end

      klass.class_eval do
        define_method(:handle_stdout) do |line, line_no|
          block.call(line, line_no)
        end
      end if block_given?

      Masamune::Commands::Shell.new(klass.new, opts).execute
    end
  end
end
