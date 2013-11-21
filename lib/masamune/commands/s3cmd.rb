module Masamune::Commands
  class S3Cmd
    DEFAULT_ATTRIBUTES =
    {
      :path     => 's3cmd',
      :options  => [],
      :extra    => [],
      :block    => nil
    }

    def initialize(attrs = {})
      DEFAULT_ATTRIBUTES.merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    def command_args
      args = []
      args << @path
      args << @options.map(&:to_a)
      args << @extra
      args.flatten
    end

    def handle_stdout(line, line_no)
      @block.call(line) if @block
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
