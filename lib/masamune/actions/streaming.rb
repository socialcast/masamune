module Masamune::Actions
  require 'masamune/commands/shell'
  require 'masamune/commands/elastic_mapreduce'
  require 'masamune/commands/streaming'

  module Streaming
    def streaming(opts = {})
      opts = opts.dup
      opts.merge!(fail_fast: true)

      command = if opts[:jobflow]
        opts.merge!(file_args: false)
        Masamune::Commands::Shell.new(
          Masamune::Commands::ElasticMapReduce.new(
            Masamune::Commands::Streaming.new(opts), opts), opts)
      else
        Masamune::Commands::Shell.new(
          Masamune::Commands::Streaming.new(opts), opts)
      end

      command.execute
    end
  end
end
