module Masamune
  class PathResolver
    extend Forwardable

    def initialize
      @paths = {}
      # TODO set context based on command, resolving path determines prefix, e.g. s3n:// vs. s3://
      @context = :default
    end

    def add_path(symbol, path, options = {})
      @paths[symbol] = path
      fs.mkdir!(path) if options[:mkdir]
      self
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

    def_delegators :@paths, :[]

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
