#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'delegate'
require 'active_support/core_ext/array'

module Masamune::Commands
  class HadoopStreaming < SimpleDelegator
    def self.default_hadoop_streaming_jar
      @default_hadoop_streaming_jar ||=
      case RUBY_PLATFORM
      when /darwin/
        Dir.glob('/usr/local/Cellar/hadoop/**/hadoop-streaming-*.jar').first
      when /linux/
        '/usr/lib/hadoop-mapreduce/hadoop-streaming.jar'
      else
        raise "unknown RUBY_PLATFORM=#{RUBY_PLATFORM}"
      end
    end

    DEFAULT_ATTRIBUTES =
    {
      path: 'hadoop',
      options: [],
      jar: default_hadoop_streaming_jar,
      input: [],
      output: nil,
      mapper: nil,
      reducer: nil,
      extra: [],
      upload: true,
      quote: false
    }.with_indifferent_access.freeze

    attr_reader :input

    def initialize(delegate, attrs = {})
      super delegate
      DEFAULT_ATTRIBUTES.merge(configuration.commands.hadoop_streaming).merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
      @input = Array.wrap(@input)
    end

    # TODO: ensure jar/ mapper/reduce exists, warn or remove if output exists
    def command_args
      args = []
      args << @path
      args << ['jar', @jar]
      args << (@quote ? @extra.map { |arg| quote_arg(arg) } : @extra)
      args << @options.map(&:to_a)
      args << ['-input', *@input] if @input
      args << ['-mapper', @mapper]
      args << ['-file', @mapper] if @upload
      args << ['-reducer', @reducer] if @reducer
      args << ['-file', @reducer] if @upload && @reducer
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
      console(format('hadoop_streaming %s -> %s (%s/%s)', @input.join(' '), @output, @mapper, @reducer))
    end

    def around_execute
      Dir.chdir(filesystem.path(:run_dir)) do
        yield
      end
    end

    private

    # FIXME: shell quoting is a separate concern
    def quote_arg(arg)
      out = arg.dup
      out.gsub!('\'\t\'', %q('"'\\\\\\\\t'"'))
      out
    end
  end
end
