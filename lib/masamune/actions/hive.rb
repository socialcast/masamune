module Masamune::Actions
  require 'masamune/commands/shell'
  require 'masamune/commands/elastic_mapreduce'
  require 'masamune/commands/hive'

  module Hive
    def hive(opts = {})
      opts = opts.dup
      opts.merge!(fail_fast: true)
      opts.merge!(jobflow: Masamune.configuration.jobflow)

      command = if opts[:jobflow]
        opts.merge!(encode: true)
        Masamune::Commands::Shell.new(
          Masamune::Commands::ElasticMapReduce.new(
            Masamune::Commands::Hive.new(opts), opts), opts)
      else
        Masamune::Commands::Shell.new(
          Masamune::Commands::Hive.new(opts), opts)
      end

      if command.interactive?
        command.replace
      else
        command.execute
      end
    end
  end
end
