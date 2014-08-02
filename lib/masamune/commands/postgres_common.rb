module Masamune::Commands
  module PostgresCommon
    def command_env
      { 'PGOPTIONS'=> '--client-min-messages=warning' }.tap do |env|
        if @pgpass_file && File.readable?(@pgpass_file)
          env['PGPASSFILE'] = @pgpass_file
        end
      end
    end
  end
end
