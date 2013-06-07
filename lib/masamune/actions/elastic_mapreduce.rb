module Masamune::Actions
  module ElasticMapreduce
    def elastic_mapreduce(opts = {})
      opts = opts.dup
      opts.merge!(jobflow: Masamune.configuration.jobflow)

      if opts.delete(:list)
        opts.merge!(mode: '--list')
      end

      command = Masamune::Commands::Shell.new(
        Masamune::Commands::ElasticMapReduce.new(nil, opts), opts)

      if command.interactive?
        command.replace
      else
        command.execute
      end
    end
  end
end
