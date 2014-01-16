require 'masamune/proxy_delegate'
require 'masamune/string_format'

module Masamune::Commands
  class Postgres
    include Masamune::ProxyDelegate
    include Masamune::StringFormat

    DEFAULT_ATTRIBUTES =
    {
      :path         => 'psql',
      :options      => [],
      :hostname     => 'localhost',
      :database     => 'postgres',
      :username     => 'postgres',
      :pgpass_file  => nil,
      :setup_files  => [],
      :schema_files => [],
      :file         => nil,
      :exec         => nil,
      :input        => nil,
      :output       => nil,
      :print        => false,
      :block        => nil,
      :csv          => false,
      :variables    => {}
    }

    def initialize(delegate, attrs = {})
      @delegate = delegate
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

    # TODO do something if file doesn't exist
    def command_env
      @pgpass_file ? {'PGPASSFILE' => @pgpass_file} : {}
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
      args << '--command=%s' % @exec if @exec
      args.flatten.compact
    end

    def before_execute
      console("psql with file #{@file}") if @file
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
      ['--file=%s' % @file].tap do |args|
        @variables.each do |key, val|
          args << '--set=%s' % "#{key.to_s}='#{val.to_s}'"
        end
      end
    end
  end
end
