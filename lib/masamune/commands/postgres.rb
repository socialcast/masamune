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

require 'masamune/string_format'
require 'masamune/commands/postgres_common'

module Masamune::Commands
  class Postgres < SimpleDelegator
    include Masamune::StringFormat
    include Masamune::Commands::PostgresCommon

    DEFAULT_ATTRIBUTES =
    {
      path: 'psql',
      options: [],
      hostname: 'localhost',
      database: 'postgres',
      username: 'postgres',
      pgpass_file: nil,
      file: nil,
      exec: nil,
      input: nil,
      output: nil,
      print: false,
      block: nil,
      csv: false,
      variables: {},
      tuple_output: false,
      debug: false
    }.with_indifferent_access.freeze

    def initialize(delegate, attrs = {})
      super delegate
      DEFAULT_ATTRIBUTES.merge(configuration.commands.postgres).merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
      raise ArgumentError, 'Cannot specify both file and exec' if @file && @exec
      @error = nil
    end

    def stdin
      @stdin ||= StringIO.new(strip_sql(@input)) if @input
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
      args << "--host=#{@hostname}" if @hostname
      args << "--dbname=#{@database}"
      args << "--username=#{@username}" if @username
      args << '--no-password'
      args << '--set=ON_ERROR_STOP=1'
      args << @options.map(&:to_a)
      args << command_args_for_file if @file
      args << "--output=#{@output}" if @output
      args << '--no-align' << '--field-separator=,' << '--pset=footer' if @csv
      args << '--pset=tuples_only' if @tuple_output
      args.flatten.compact
    end

    def before_execute
      console("psql with file #{@file}") if @file
      if @debug && (output = rendered_template || @file)
        logger.debug("#{output}:\n" + File.read(output))
      end

      if @exec
        console("postgres exec '#{strip_sql(@exec)}' #{'into ' + @output if @output}")
        @file = exec_file
      end
    end

    def handle_stdout(line, _line_no)
      if line =~ /\A#{prompt}/
        logger.debug(line)
      else
        @block.call(line) if @block
        console(line) if print?
      end
    end

    def handle_stderr(line, _line_no)
      @error = line.split(/ERROR:\s*/).last if line =~ /ERROR:/
      logger.debug(line)
    end

    def failure_message(_status)
      @error || 'psql failed without error'
    end

    def prompt
      @database + '=>'
    end

    private

    def template_file?
      @file =~ /\.erb\Z/
    end

    def command_args_for_file
      template_file? ? command_args_for_template : command_args_for_simple_file
    end

    def command_args_for_simple_file
      ["--file=#{@file}"].tap do |args|
        @variables.each do |key, val|
          args << "--set=#{key}='#{val}'"
        end
      end
    end

    def command_args_for_template
      ["--file=#{rendered_template}"]
    end

    def exec_file
      @exec_file ||= Tempfile.create('masamune_psql_input').tap do |tmp|
        tmp.write(@exec)
        tmp.close
      end.path
    end

    def rendered_template
      return unless template_file?
      @rendered_template ||= Masamune::Template.render_to_file(@file, @variables)
    end
  end
end
