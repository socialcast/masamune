module Masamune::Actions
  module S3Cmd
    include Masamune::Commands::S3Cmd::ClassMethods

    def s3cmd(*args, &block)
      opts = args.last.is_a?(Hash) ? args.pop : {}
      opts = opts.to_hash.symbolize_keys
      opts.reverse_merge!(configuration.s3cmd) if configuration
      opts.merge!(extra: Array.wrap(args))
      opts.merge!(block: block.to_proc) if block_given?

      command = Masamune::Commands::S3Cmd.new(context, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, opts)
      command = Masamune::Commands::Shell.new(command, opts)

      command.execute
    end

    def s3_sync(src, dst)
      s3cmd('sync', s3b(src), s3b(dst, :dir => true))
    end
  end
end
