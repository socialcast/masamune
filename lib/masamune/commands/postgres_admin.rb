module Masamune::Commands
  class PostgresAdmin
    attr_accessor :action, :database

    def initialize(opts = {})
      self.action     = opts[:action]
      self.database   = opts[:database]
    end

    def command_env
      configuration[:pgpass_file] ? {'PGPASSFILE' => configuration[:pgpass_file]} : {}
    end

    def command_args
      raise ArgumentError, ':database must be given' unless database
      args = []
      args << command_path
      args << '--host=%s' % configuration[:hostname] if configuration[:hostname]
      args << '--username=%s' % configuration[:username] if configuration[:username]
      args << '--no-password'
      args << database
      args.flatten.compact
    end

    private

    def command_path
      case action
      when :create
        configuration[:create_db_path]
      when :drop
        configuration[:drop_db_path]
      else
        raise ArgumentError, ':action must be :create or :drop'
      end
    end

    def configuration
      Masamune.configuration.postgres_admin.merge(Masamune.configuration.postgres)
    end
  end
end
