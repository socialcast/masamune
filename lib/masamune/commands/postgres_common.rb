module Masamune::Commands
  module PostgresCommon
    def command_env
      @pgpass_file && File.readable?(@pgpass_file) ? {'PGPASSFILE' => @pgpass_file} : {}
    end
  end
end
