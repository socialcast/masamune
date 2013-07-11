module Masamune::Commands
  class S3Cmd
    attr_accessor :extra, :block

    def initialize(opts = {})
      self.extra    = opts[:extra]
      self.block    = opts[:block]
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
