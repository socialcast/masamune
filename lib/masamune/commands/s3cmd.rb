module Masamune::Commands
  class S3Cmd
    MAX_RETRIES = 3
    DEFAULT_BACKOFF = 5

    attr_accessor :extra, :block, :backoff

    def initialize(opts = {})
      self.extra    = opts[:extra]
      self.block    = opts[:block]
      self.backoff  = opts.fetch(:backoff, DEFAULT_BACKOFF)
      @retry_count = 0
    end

    def command_args
      args = []
      args << Masamune.configuration.s3cmd[:path]
      args << Masamune.configuration.s3cmd[:options].map(&:to_a)
      args << extra
      args.flatten
    end

    def handle_stdout(line, line_no)
      block.call(line) if block
    end

    def around_execute(&block)
      begin
        yield
      rescue
        sleep backoff
        @retry_count += 1
        retry unless @retry_count > MAX_RETRIES
      end
    end

    module ClassMethods
      def s3n(file, options = {})
        file.dup.tap do |out|
          out.sub!(%r{\As3://}, 's3n://')
          out.sub!(%r{/?\z}, '/') if options[:dir]
        end
      end

      def s3b(file, options = {})
        file.dup.tap do |out|
          out.sub!(%r{\As3n://}, 's3://')
          out.sub!(%r{/?\z}, '/') if options[:dir]
        end
      end
    end
  end
end
