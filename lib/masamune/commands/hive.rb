require 'masamune/proxy_delegate'
require 'masamune/string_format'
require 'masamune/commands/shell'

module Masamune::Commands
  class Hive
    include Masamune::StringFormat
    include Masamune::ProxyDelegate

    PROMPT = 'hive>'

    DEFAULT_ATTRIBUTES =
    {
      :path         => 'hive',
      :options      => [],
      :database     => 'default',
      :setup_files  => [],
      :schema_files => [],
      :file         => nil,
      :exec         => nil,
      :input        => nil,
      :output       => nil,
      :print        => false,
      :block        => nil,
      :variables    => {},
      :rollback     => nil
    }

    def initialize(delegate, attrs = {})
      @delegate = delegate
      DEFAULT_ATTRIBUTES.merge(configuration.hive).merge(attrs).each do |name, value|
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
      args << ['--database', @database] if @database
      args << @options.map(&:to_a)
      args << load_setup_and_schema_files.map(&:to_a)
      args << ['-f', @file] if @file
      @variables.each do |key, val|
        args << ['-d', "#{key.to_s}=#{val.to_s}"]
      end
      args.flatten
    end

    def before_execute
      if @file
        console("hive with file #{@file}")
      end

      if @exec
        console("hive exec '#{strip_sql(@exec)}' #{'into ' + @output if @output}")
      end

      if @output
        @tmpfile = Tempfile.new('masamune')
      end
    end

    def around_execute(&block)
      Dir.chdir(filesystem.path(:run_dir)) do
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
          console(line) if print?
        end
      end
    end

    def handle_failure(status)
      if @rollback
        logger.error('rolling back')
        @rollback.call
      end
    end

    def load_setup_and_schema_files
      files = []
      files << @setup_files if @setup_files
      files << @schema_files if @schema_files
      files.flatten.compact.map { |file| {'-i' => file} }
    end
  end
end
