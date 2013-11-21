require 'masamune/client'

module Masamune::Commands
  class PostgresAdmin
    include Masamune::ClientBehavior

    DEFAULT_ATTRIBUTES =
    {
      :create_db_path => 'createdb',
      :drop_db_path   => 'dropdb',
      :options        => [],
      :hostname       => 'localhost',
      :username       => 'postgres',
      :pgpass_file    => nil,
      :action         => nil,
      :database       => nil
    }

    def initialize(attrs = {})
      DEFAULT_ATTRIBUTES.merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    # TODO do something if file doesn't exist
    def command_env
      @pgpass_file ? {'PGPASSFILE' => @pgpass_file} : {}
    end

    def command_args
      raise ArgumentError, ':database must be given' unless @database
      args = []
      args << command_path
      args << '--host=%s' % @hostname if @hostname
      args << '--username=%s' % @username if @username
      args << '--no-password'
      args << @database
      args.flatten.compact
    end

    private

    def command_path
      case @action
      when :create
        @create_db_path
      when :drop
        @drop_db_path
      else
        raise ArgumentError, ':action must be :create or :drop'
      end
    end
  end
end
