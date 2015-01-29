require 'masamune/proxy_delegate'
require 'masamune/string_format'
require 'masamune/commands/shell'
require 'csv'

module Masamune::Commands
  class Hive
    include Masamune::StringFormat
    include Masamune::ProxyDelegate

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
      :input          => nil,
      :output         => nil,
      :print          => false,
      :block          => nil,
      :variables      => {},
      :buffer         => nil,
      :delimiter      => "\001",
      :csv            => false,
      :debug          => false
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
      args << load_setup_files.map(&:to_a)
      args << command_args_for_file if @file
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
        @buffer ||= Tempfile.new('masamune')
      end
    end

    def around_execute(&block)
      Dir.chdir(filesystem.path(:run_dir)) do
        yield
      end
    end

    def after_execute
      @buffer.flush if @buffer && @buffer.respond_to?(:flush)
      @buffer.close if @buffer && @buffer.respond_to?(:close)

      filesystem.move_file_to_file(@buffer.path, @output) if @output && @buffer && @buffer.respond_to?(:path)
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
      files = []
      @setup_files.each do |path|
        filesystem.glob_sort(path, order: :basename).each do |file|
          files << file
        end
      end
      files.flatten.compact.map { |file| {'-i' => file} }
    end

    def command_args_for_file
      @file =~ /\.erb\Z/ ? command_args_for_template : command_args_for_simple_file
    end

    def command_args_for_simple_file
      filesystem.copy_file_to_dir(@file, filesystem.get_path(:tmp_dir))
      ['-f', filesystem.get_path(:tmp_dir, File.basename(@file))].tap do |args|
        @variables.each do |key, val|
          args << ['-d', "#{key.to_s}=#{val.to_s}"]
        end
      end
    end

    def command_args_for_template
      rendered_file = Masamune::Template.render_to_file(@file, @variables)
      logger.debug("#{@file}:\n" + File.read(rendered_file)) if @debug
      filesystem.copy_file_to_dir(rendered_file, filesystem.get_path(:tmp_dir))
      ['-f', filesystem.get_path(:tmp_dir, File.basename(rendered_file))]
    end
  end
end
