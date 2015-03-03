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

require 'masamune/string_format'

module Masamune::Commands
  class Postgres < SimpleDelegator
    include Masamune::StringFormat
    include Masamune::Commands::PostgresCommon

    DEFAULT_ATTRIBUTES =
    {
      :path           => 'psql',
      :options        => [],
      :hostname       => 'localhost',
      :database       => 'postgres',
      :username       => 'postgres',
      :pgpass_file    => nil,
      :file           => nil,
      :exec           => nil,
      :input          => nil,
      :output         => nil,
      :print          => false,
      :block          => nil,
      :csv            => false,
      :variables      => {},
      :tuple_output   => false,
      :debug          => false
    }

    def initialize(delegate, attrs = {})
      super delegate
      DEFAULT_ATTRIBUTES.merge(configuration.postgres).merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    def stdin
      if @input
        @stdin ||= StringIO.new(strip_sql(@input))
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
      args << '--host=%s' % @hostname if @hostname
      args << '--dbname=%s' % @database
      args << '--username=%s' % @username if @username
      args << '--no-password'
      args << @options.map(&:to_a)
      args << command_args_for_file if @file
      args << '--output=%s' % @output if @output
      args << '--no-align' << '--field-separator=,' << '--pset=footer' if @csv
      args << '--command=%s' % strip_sql(@exec) if @exec
      args << '--pset=tuples_only' if @tuple_output
      args.flatten.compact
    end

    def before_execute
      console("psql with file #{@file}") if @file
      if @debug and output = @rendered_file || @file
        logger.debug("#{output}:\n" + File.read(output))
      end
    end

    def handle_stdout(line, line_no)
      if line =~ /\A#{prompt}/
        logger.debug(line)
      else
        @block.call(line) if @block
        console(line) if print?
      end
    end

    def prompt
      @database + '=>'
    end

    private

    def command_args_for_file
      @file =~ /\.erb\Z/ ? command_args_for_template : command_args_for_simple_file
    end

    def command_args_for_simple_file
      ['--file=%s' % @file].tap do |args|
        @variables.each do |key, val|
          args << '--set=%s' % "#{key.to_s}='#{val.to_s}'"
        end
      end
    end

    def command_args_for_template
      @rendered_file = Masamune::Template.render_to_file(@file, @variables)
      ['--file=%s' % @rendered_file]
    end
  end
end
