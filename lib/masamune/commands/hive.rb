#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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
require 'csv'

require 'masamune/string_format'
require 'masamune/commands/shell'

module Masamune::Commands
  class Hive < SimpleDelegator
    include Masamune::StringFormat

    PROMPT = 'hive>'

    DEFAULT_ATTRIBUTES =
    {
      :path           => 'hive',
      :options        => [],
      :database       => 'default',
      :setup_files    => [],
      :schema_files   => [],
      :file           => nil,
      :exec           => nil,
      :output         => nil,
      :print          => false,
      :block          => nil,
      :variables      => {},
      :buffer         => nil,
      :service        => false,
      :delimiter      => "\001",
      :csv            => false,
      :debug          => false
    }

    def initialize(delegate, attrs = {})
      super delegate
      DEFAULT_ATTRIBUTES.merge(configuration.hive).merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    def interactive?
      !(@exec || @file)
    end

    def print?
      @print
    end

    def command_args
      args = []
      args << @path
      if @service
        args << ['--service', 'hiveserver']
      else
        args << ['--database', @database] if @database && !@service
        args << @options.map(&:to_a)
        args << load_setup_files.map(&:to_a)
        args << command_args_for_file if @file
      end
      args.flatten
    end

    def before_execute
      if @file
        console("hive with file #{@file}")
      end

      if @debug and output = @rendered_file || @file
        logger.debug("#{output}:\n" + File.read(output))
      end

      if @exec
        console("hive exec '#{strip_sql(@exec)}' #{'into ' + @output if @output}")
        @file = exec_file
      end

      if @output
        @buffer = Tempfile.new('masamune')
      end
    end

    def around_execute(&block)
      Dir.chdir(filesystem.path(:run_dir)) do
        yield
      end
    end

    def after_execute
      return unless @buffer
      @buffer.flush unless @buffer.closed?
      @buffer.close unless @buffer.closed?
      return unless @output

      filesystem.move_file_to_file(@buffer.path, @output)
    end

    # FIXME use temporary tables with delimiters for CSV output format
    def handle_stdout(line, line_no)
      if line =~ /\A#{PROMPT}/
        logger.debug(line)
      else
        @block.call(line) if @block

        if @buffer
          @buffer.puts(@csv ? line.split(@delimiter).map { |row| encode_row(row) }.to_csv : line)
        else
          console(line) if print?
        end
      end
    end

    def encode_row(row)
      row unless row == 'NULL' || row == ''
    end

    def load_setup_files
      files = @setup_files.map do |path|
        filesystem.glob_sort(path, order: :basename)
      end
      files.flatten.compact.map { |file| {'-i' => file} }
    end

    def command_args_for_file
      @file =~ /\.erb\Z/ ? command_args_for_template : command_args_for_simple_file
    end

    def command_args_for_simple_file
      filesystem.copy_file_to_file(@file, remote_file)
      ['-f', remote_file].tap do |args|
        @variables.each do |key, val|
          args << ['-d', "#{key.to_s}=#{val.to_s}"]
        end
      end
    end

    def command_args_for_template
      @rendered_file = Masamune::Template.render_to_file(@file, @variables)
      filesystem.copy_file_to_file(@rendered_file, remote_file)
      ['-f', remote_file]
    end

    private

    def remote_file
      @remote_file ||= File.join(filesystem.mktempdir!(:tmp_dir), filesystem.basename(@file)).gsub(/.erb\z/,'')
    end

    def exec_file
      Tempfile.new('masamune').tap do |tmp|
        tmp.write(@exec)
        tmp.flush
      end.path
    end
  end
end
