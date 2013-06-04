module Masamune::Actions
  module Execute
    require 'masamune/commands/shell'

    def execute(*args, &block)
      opts = args.last.is_a?(Hash) ? args.pop : {}

      klass = Class.new
      klass.define_method(:command_args) do
        args
      end

      klass.define_method(:handle_stdout) do |line, line_no|
        block.call(line, line_no)
      end if block_given?

      Masamune::Commands::Shell.new(klass.new, opts).execute
    end
  end
end
