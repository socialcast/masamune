require 'masamune'
require 'thor'

module Masamune::Tasks
  class PostgresThor < Thor
    include Masamune::Thor
    include Masamune::Actions::Postgres

    # FIXME need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :postgres

    desc 'psql', 'Launch a Postgres session'
    method_option :file, :aliases => '-f', :desc => 'SQL from files'
    method_option :exec, :aliases => '-e', :desc => 'SQL from command line'
    method_option :output, :aliases => '-o', :desc => 'Save SQL output to file'
    method_option :csv, :type => :boolean, :desc => 'Report SQL output in CSV format', :default => false
    def psql_exec
      postgres_options = options.dup
      postgres_options.merge!(print: true)
      postgres_options.merge!(ifs: "\t", ofs: ',') if options[:csv]
      postgres(postgres_options)
    end
    default_task :psql_exec

    no_tasks do
      def log_enabled?
        false
      end
    end
  end
end
