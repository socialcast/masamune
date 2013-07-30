module Masamune::Actions
  module Hive
    def hive(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys

      opts.merge!(jobflow: Masamune.configuration.jobflow)
      opts.merge!(block: block.to_proc) if block_given?
      opts.merge!(ifs: "\t", ofs: ',') if opts.delete(:csv)

      command = Masamune::Commands::Hive.new(opts)
      command = Masamune::Commands::ElasticMapReduce.new(command, opts) if opts[:jobflow]
      command = Masamune::Commands::LineFormatter.new(command, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      if command.interactive?
        command.replace
      else
        command.execute
      end
    end
  end
end
