require 'masamune/has_context'
require 'masamune/string_format'
require 'masamune/commands/shell'

module Masamune::Commands
  class Hive
    include Masamune::HasContext
    include Masamune::StringFormat

    PROMPT = 'hive>'

    DEFAULT_ATTRIBUTES =
    {
      :path       => 'hive',
      :options    => [],
      :database   => 'default',
      :file       => nil,
      :exec       => nil,
      :input      => nil,
      :output     => nil,
      :print      => false,
      :block      => nil,
      :variables  => {},
      :rollback   => nil
    }

    def initialize(attrs  = {})
      DEFAULT_ATTRIBUTES.merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    def stdin
      if @input || @exec
        @stdin ||= StringIO.new(strip_sql(@input || @exec))
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
      args << @options.map(&:to_a)
      args << ['-f', @file] if @file
      @variables.each do |key, val|
        args << ['-d', "#{key.to_s}=#{val.to_s}"]
      end
      args.flatten
    end

    def before_execute
      if @file
        print("hive with file #{@file}")
      end

      if @exec
        print("hive exec '#{strip_sql(@exec)}' #{'into ' + @output if @output}")
      end

      if @output
        @tmpfile = Tempfile.new('masamune')
      end
    end

    def around_execute(&block)
      Dir.chdir(filesystem.path(:var_dir)) do
        yield
      end
    end

    def after_execute
      if @output
        @tmpfile.close
        filesystem.move_file(@tmpfile.path, @output)
        @tmpfile.unlink
      end
    end

    def handle_stdout(line, line_no)
      if line =~ /\A#{PROMPT}/
        logger.debug(line)
      else
        @block.call(line) if @block

        if @tmpfile
          @tmpfile.puts(line)
        else
          print(line) if print?
        end
      end
    end

    def handle_failure(status)
      if @rollback
        logger.error('rolling back')
        @rollback.call
      end
    end
  end
end
