module Masamune::Commands
  class PostgresAdmin
    attr_accessor :action, :database

    def initialize(opts = {})
      self.action     = opts[:action]
      self.database   = opts[:database]
    end

    def command_args
      raise ArgumentError, ':database must be given' unless database
      args = []
      args << "PGPASSFILE=#{configuration[:pgpass_file]}" if configuration[:pgpass_file]
      args << command_path
      args << ['--host', configuration[:hostname]]
      args << ['--username', configuration[:username]]
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
