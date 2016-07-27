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

module Masamune::Actions
  module Hive
    include Masamune::Transform::DefineSchema

    extend ActiveSupport::Concern

    def hive(opts = {}, &block)
      opts = opts.to_hash.symbolize_keys
      opts[:block] = block.to_proc if block_given?

      command = Masamune::Commands::Hive.new(environment, opts)
      command = Masamune::Commands::AwsEmr.new(command, opts.except(:extra)) if configuration.commands.aws_emr[:cluster_id]
      command = Masamune::Commands::RetryWithBackoff.new(command, configuration.commands.hive.slice(:retries, :backoff).merge(opts))
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    # TODO: warn or error if database is not defined
    def create_hive_database_if_not_exists
      return if configuration.commands.hive[:database] == 'default'
      sql = []
      sql << %(CREATE DATABASE IF NOT EXISTS #{configuration.commands.hive[:database]})
      sql << %(LOCATION "#{configuration.commands.hive[:location]}") if configuration.commands.hive[:location]
      hive(exec: sql.join(' ') + ';', database: nil)
    end

    def load_hive_schema
      transform = define_schema(catalog, :hive)
      hive(file: transform.to_file)
    rescue => e
      logger.error(e)
      logger.error('Could not load schema')
      logger.error("\n" + transform.to_s)
      exit
    end

    included do |base|
      base.after_initialize do |thor, options|
        next unless options[:initialize]
        thor.create_hive_database_if_not_exists
        thor.load_hive_schema
      end if defined?(base.after_initialize)

      base.after_initialize(:later) do |thor, options|
        next unless options[:dry_run]
        raise ::Thor::InvocationError, 'Dry run of hive failed' unless thor.hive(exec: 'SHOW TABLES;', safe: true, fail_fast: false).success?
      end if defined?(base.after_initialize)
    end
  end
end
