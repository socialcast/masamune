require 'masamune/has_context'
require 'masamune/actions/postgres'

module Masamune::Helpers
  class Postgres
    include Masamune::HasContext
    include Masamune::Actions::Postgres
    include Masamune::Actions::PostgresAdmin

    def initialize(context)
      self.context = context
    end

    def database_exists?
      postgres(exec: 'SELECT version();', fail_fast: false).success?
    end

    def table_exists?(table)
      return unless database_exists?
      tables.include?(table)
    end

    def tables
      @table_cache ||= begin
        tables = Set.new
        postgres(exec: 'SELECT table_name FROM information_schema.tables;', tuple_output: true) do |line|
          table = line.strip
          next if table =~ /\Apg_/
          tables << table
        end
        tables
      end
    end

    def clear!
      @table_cache = nil
    end
  end
end
