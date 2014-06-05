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

    # TODO consider bulk operation + caching
    def last_modified_at(table, options = {})
      column = options[:last_modified_at]
      return unless column
      return unless table_exists?(table)
      value = nil
      postgres(exec: "SELECT MAX(#{column}) FROM #{table};", tuple_output: true) do |line|
        begin
          value = Time.parse(line.strip).at_beginning_of_minute.utc
        rescue ArgumentError
        end
      end
      value
    end

    def drop_table(table)
      return unless table_exists?(table)
      postgres(exec: "DROP TABLE #{table};", fail_fast: true).success?
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
