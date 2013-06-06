require 'masamune'
require 'thor'

module Masamune::Tasks
  class HiveThor < Thor
    include Masamune::Thor
    include Masamune::Actions::Hive

    desc 'hive', 'Launch a Hive session'
    method_option :file, :aliases => '-f', :desc => 'SQL from files'
    method_option :exec, :aliases => '-e', :desc => 'SQL from command line'
    def hive_exec
      hive(options)
    end
    default_task :hive_exec
  end
end
