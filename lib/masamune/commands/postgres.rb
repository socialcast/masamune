require 'masamune/string_format'

module Masamune::Commands
  class Postgres
    include Masamune::StringFormat

    attr_accessor :file, :exec, :input, :output, :print, :block, :variables

    def initialize(opts = {})
      self.file       = opts[:file]
      self.exec       = opts[:exec]
      self.output     = opts[:output]
      self.print      = opts.fetch(:print, false)
      self.block      = opts[:block]
      self.variables  = opts.fetch(:variables, {})
    end

    def exec=(sql = nil)
      return unless sql
      self.input = @exec = strip_sql(sql)
    end

    def stdin
      if input
        @stdin ||= StringIO.new(input)
      end
    end

    def interactive?
      !(exec || file)
    end

    def print?
      self.print
    end

    def command_env
      configuration[:pgpass_file] ? {'PGPASSFILE' => configuration[:pgpass_file]} : {}
    end

    def command_args
      args = []
      args << configuration[:path]
      args << '--host=%s' % configuration[:hostname] if configuration[:hostname]
      args << '--dbname=%s' % configuration[:database]
      args << '--username=%s' % configuration[:username] if configuration[:username]
      args << '--no-password'
      args << configuration[:options].map(&:to_a)
      args << '--file=%s' % file if file
      args << '--output=%s' % output if output
      variables.each do |key, val|
        args << '--set=%s' % "#{key.to_s}='#{val.to_s}'"
      end
      args.flatten.compact
    end

    def before_execute
      Masamune.print("psql with file #{file}") if file
    end

    def handle_stdout(line, line_no)
      if line =~ /\A#{prompt}/
        Masamune.logger.debug(line)
      else
        block.call(line) if block
        Masamune::print(line) if print?
      end
    end

    def prompt
      configuration[:database] + '=>'
    end

    private

    def configuration
      Masamune.configuration.postgres
    end
  end
end
