require 'masamune/client'
require 'masamune/string_format'

module Masamune::Commands
  class Postgres
    include Masamune::ClientBehavior
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
      :variables    => {}
    }

    def initialize(attrs = {})
      DEFAULT_ATTRIBUTES.merge(attrs).each do |name, value|
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

=begin
    def command_bin
      @path
    end
=end

    def command_args
      args = []
      args << @path
      args << '--host=%s' % @hostname if @hostname
      args << '--dbname=%s' % @database
      args << '--username=%s' % @username if @username
      args << '--no-password'
      args << @options.map(&:to_a)
      args << '--file=%s' % @file if @file
      args << '--output=%s' % @output if @output
      @variables.each do |key, val|
        args << '--set=%s' % "#{key.to_s}='#{val.to_s}'"
      end
      args << 'command=%s' % @exec if @exec
      args.flatten.compact
    end

    def before_execute
      print("psql with file #{@file}") if @file
    end

    def handle_stdout(line, line_no)
      if line =~ /\A#{prompt}/
        logger.debug(line)
      else
        @block.call(line) if @block
        print(line) if print?
      end
    end

    def prompt
      @database + '=>'
    end
  end
end
