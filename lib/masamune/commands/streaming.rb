require 'active_support/core_ext/array'

require 'masamune/context'

module Masamune::Commands
  class Streaming
    include Masamune::ContextBehavior

    # FIXME make a better guess with Find
    def self.default_hadoop_streaming_jar
      @default_hadoop_streaming_jar ||=
      case RUBY_PLATFORM
      when /darwin/
        '/usr/local/Cellar/hadoop/1.1.2/libexec/contrib/streaming/hadoop-streaming-1.1.2.jar'
      when /linux/
        '/usr/lib/hadoop-mapreduce/hadoop-streaming.jar'
      else
        raise 'hadoop_streaming_jar not found'
      end
    end

    # TODO rename extra_args to extra
    DEFAULT_ATTRIBUTES =
    {
      :path         => 'hadoop',
      :options      => [],
      :jar          => default_hadoop_streaming_jar,
      :input        => [],
      :output       => nil,
      :mapper       => nil,
      :reducer      => nil,
      :extra_args   => [],
      :file_args    => true,
      :quote        => false
    }

    attr_reader :input

    def initialize(attrs = {})
      DEFAULT_ATTRIBUTES.merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
      @input = Array.wrap(@input)
    end

    def command_args
      args = []
      args << @path
      args << ['jar', @jar]
      args << (@quote ? @extra_args.map { |arg| quote_arg(arg) } : @extra_args)
      args << @options.map(&:to_a)
      args << ['-input', *@input] if @input
      args << ['-mapper', @mapper]
      args << ['-file', @mapper] if @file_args
      args << ['-reducer', @reducer]
      args << ['-file', @reducer] if @file_args
      args << ['-output', @output]
      args.flatten
    end

    def before_execute
      @input.reject! do |path|
        if filesystem.exists?(path)
          false
        else
          logger.debug("Removing missing input #{path} from streaming command")
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
      print("streaming %s -> %s (%s/%s)" % [@input.join(' '), @output, @mapper, @reducer])
    end

    def around_execute(&block)
      Dir.chdir(filesystem.path(:var_dir)) do
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
