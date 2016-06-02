#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

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
      @database_exists ||= postgres(exec: 'SELECT version();', fail_fast: false, retries: 0).success?
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
      postgres(exec: 'SELECT table_name FROM information_schema.tables;', tuple_output: true, retries: 0) do |line|
        table = line.strip
        next if table.start_with?('pg_')
        @cache[table] ||= nil
      end
    end

    def update_table_last_modified_at(table, column)
      return if @cache[table].present?
      postgres(exec: "SELECT MAX(#{column}) FROM #{table};", tuple_output: true, retries: 0) do |line|
        last_modified_at = line.strip
        @cache[table] = parse_date_time(last_modified_at) unless last_modified_at.blank?
      end
    end

    def parse_date_time(value)
      Time.parse(value).at_beginning_of_minute.utc
    rescue ArgumentError
      nil
    end
  end
end
