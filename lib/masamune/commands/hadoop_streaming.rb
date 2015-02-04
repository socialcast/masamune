require 'delegate'
require 'active_support/core_ext/array'

module Masamune::Commands
  class HadoopStreaming < SimpleDelegator
    def self.default_hadoop_streaming_jar
      @default_hadoop_streaming_jar ||=
      case RUBY_PLATFORM
      when /darwin/
        Dir.glob('/usr/local/Cellar/hadoop/*/libexec/contrib/streaming/hadoop-streaming-*.jar').first
      when /linux/
        '/usr/lib/hadoop-mapreduce/hadoop-streaming.jar'
      else
        raise "unknown RUBY_PLATFORM=#{RUBY_PLATFORM}"
      end
    end

    DEFAULT_ATTRIBUTES =
    {
      :path         => 'hadoop',
      :options      => [],
      :jar          => default_hadoop_streaming_jar,
      :input        => [],
      :output       => nil,
      :mapper       => nil,
      :reducer      => nil,
      :extra        => [],
      :upload       => true,
      :quote        => false
    }

    attr_reader :input

    def initialize(delegate, attrs = {})
      super delegate
      DEFAULT_ATTRIBUTES.merge(configuration.hadoop_streaming).merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
      @input = Array.wrap(@input)
    end

    # TODO ensure jar/ mapper/reduce exists, warn or remove if output exists
    def command_args
      args = []
      args << @path
      args << ['jar', @jar]
      args << (@quote ? @extra.map { |arg| quote_arg(arg) } : @extra)
      args << @options.map(&:to_a)
      args << ['-input', *@input] if @input
      args << ['-mapper', @mapper]
      args << ['-file', @mapper] if @upload
      args << ['-reducer', @reducer]
      args << ['-file', @reducer] if @upload
      args << ['-output', @output]
      args.flatten
    end

    def before_execute
      @input.reject! do |path|
        if filesystem.exists?(path)
          false
        else
          logger.debug("Removing missing input #{path} from hadoop_streaming command")
          true
        end
      end
      @input.map! do |path|
        if path =~ /part_.*\Z/ || path =~ /\..*\Z/
          path
        elsif path =~ %r{/\Z}
          path + '*'
        else
          path + '/*'
        end
      end
      console("hadoop_streaming %s -> %s (%s/%s)" % [@input.join(' '), @output, @mapper, @reducer])
    end

    def around_execute(&block)
      Dir.chdir(filesystem.path(:run_dir)) do
        yield
      end
    end

    private

    # FIXME shell quoting is a separate concern
    def quote_arg(arg)
      out = arg.dup
      out.gsub!(%q('\t'), %q('"'\\\\\\\\t'"'))
      out
    end
  end
end
