require 'masamune/has_environment'
require 'masamune/actions/postgres'

module Masamune::Helpers
  class Postgres
    include Masamune::HasEnvironment
    include Masamune::Actions::Postgres
    include Masamune::Actions::PostgresAdmin

    def initialize(environment)
      self.environment = environment
      clear!
    end

    def clear!
      @cache = {}
    end

    def database_exists?
      postgres(exec: 'SELECT version();', fail_fast: false).success?
    end

    def table_exists?(table)
      return unless database_exists?
      tables.include?(table)
    end

    def table_last_modified_at(table, options = {})
      column = options[:last_modified_at]
      return unless column
      return unless table_exists?(table)
      update_table_last_modified_at(table, column)
      @cache[table]
    end

    def tables
      update_tables
      @cache.keys
    end

    private

    def update_tables
      return unless @cache.empty?
      postgres(exec: 'SELECT table_name FROM information_schema.tables;', tuple_output: true) do |line|
        table = line.strip
        next if table =~ /\Apg_/
        @cache[table] ||= nil
      end
    end

    def update_table_last_modified_at(table, column)
      return if @cache[table].present?
      postgres(exec: "SELECT MAX(#{column}) FROM #{table};", tuple_output: true) do |line|
        begin
          @cache[table] = Time.parse(line.strip).at_beginning_of_minute.utc
        rescue ArgumentError
        end
      end
    end
  end
end
