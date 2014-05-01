require 'masamune'
require 'thor'

module Masamune::Tasks
  class HiveThor < Thor
    include Masamune::Thor
    include Masamune::Actions::ElasticMapreduce
    include Masamune::Actions::Hive

    # FIXME need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :hive

    desc 'hive', 'Launch a Hive session'
    method_option :file, :aliases => '-f', :desc => 'SQL from files'
    method_option :exec, :aliases => '-e', :desc => 'SQL from command line'
    method_option :output, :aliases => '-o', :desc => 'Save SQL output to file'
    method_option :delimiter, :desc => 'Hive row format delimiter', :default => "\001"
    method_option :csv, :type => :boolean, :desc => 'Report SQL output in CSV format', :default => false
    method_option :variables, :aliases => '-D', :type => :hash, :desc => 'Variables to substitute in SQL', :default => {}
    method_option :retry, :type => :boolean, :desc => 'Retry SQL query in event of failure', :default => false
    def hive_exec
      hive_options = options.dup
      hive_options.merge!(print: true)
      hive_options.merge!(retries: 0) unless options[:retry]

      if options[:file]
        remote_file = fs.path(:tmp_dir, File.basename(options[:file]))
        fs.copy_file(options[:file], fs.path(:tmp_dir)) unless options[:file] == remote_file
        hive_options.merge!(file: remote_file)
      end

      hive(hive_options)
    end
    default_task :hive_exec

    no_tasks do
      def log_enabled?
        false
      end
    end
  end
end
