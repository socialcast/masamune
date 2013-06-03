module Masamune::Actions
  require 'masamune/commands/hive'
  module Hive
    def hive(opts = {})
      command = Masamune::Commands::Hive.new(opts)
      if command.interactive?
        command.replace
      else
        command.execute
      end
    end
  end
end
