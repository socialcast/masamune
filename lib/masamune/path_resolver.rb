module Masamune
  class PathResolver
    def initialize
      @paths = {}
      # TODO set context based on command, resolving path determines prefix, e.g. s3n:// vs. s3://
      @context = :default
    end

    def add_path(symbol, path, options = {})
      @paths[symbol] = path
      Masamune.configuration.filesystem.mkdir!(path) if options[:mkdir]
      self
    end

    def get_path(symbol)
      @paths[symbol]
    end

    def type(path)
      case path
      when %r{\Afile://}, %r{\Ahdfs://}
        :hdfs
      when %r{\As3n?://}
        :s3
      else
        :local
      end
    end

=begin
    module ClassMethods
      def s3b(file, options = {})
        file.dup.tap do |out|
          out.sub!(%r{\As3n://}, 's3://')
          out.sub!(%r{/?\z}, '/') if options[:dir]
        end
      end
    end
=end
  end
end
