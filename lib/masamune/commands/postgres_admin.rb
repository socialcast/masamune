require 'masamune/proxy_delegate'

module Masamune::Commands
  class PostgresAdmin
    include Masamune::ProxyDelegate
    include Masamune::Commands::PostgresCommon

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

    def initialize(delegate, attrs = {})
      @delegate = delegate
      DEFAULT_ATTRIBUTES.merge(configuration.postgres).merge(configuration.postgres_admin).merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
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
        [@create_db_path]
      when :drop
        [@drop_db_path, '--if-exists']
      else
        raise ArgumentError, ':action must be :create or :drop'
      end
    end
  end
end
