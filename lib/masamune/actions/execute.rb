module Masamune::Actions
  module Execute
    require 'masamune/commands/shell'

    def execute(*a)
      Masamune::Commands::Shell.new(a).execute
    end
  end
end
