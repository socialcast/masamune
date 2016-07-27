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

require 'masamune/transform/define_schema'
require 'masamune/actions/postgres_admin'

module Masamune::Actions
  module Postgres
    extend ActiveSupport::Concern

    include Masamune::Actions::PostgresAdmin
    include Masamune::Transform::DefineSchema

    def postgres(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys
      opts[:block] = block.to_proc if block_given?

      command = Masamune::Commands::Postgres.new(environment, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, configuration.commands.postgres.slice(:retries, :backoff).merge(opts))
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    def create_postgres_database_if_not_exists
      if configuration.commands.postgres.key?(:database)
        postgres_admin(action: :create, database: configuration.commands.postgres[:database], safe: true) unless postgres_helper.database_exists?
      end
    end

    def load_postgres_setup_files
      configuration.commands.postgres[:setup_files].each do |file|
        configuration.with_quiet do
          postgres(file: file, retries: 0)
        end
      end if configuration.commands.postgres.key?(:setup_files)
    end

    def load_postgres_schema
      transform = define_schema(catalog, :postgres)
      postgres(file: transform.to_file, retries: 0)
    rescue => e
      logger.error(e)
      logger.error('Could not load schema')
      logger.error("\n" + transform.to_s)
      exit
    end

    included do |base|
      base.after_initialize do |thor, options|
        next unless options[:initialize]
        thor.create_postgres_database_if_not_exists
        thor.load_postgres_setup_files
        thor.load_postgres_schema
      end if defined?(base.after_initialize)
    end
  end
end
