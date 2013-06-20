require 'masamune'
require 'thor'

module Masamune::Tasks
  class ElasticMapreduceThor < Thor
    include Masamune::Thor
    include Masamune::Actions::ElasticMapreduce

    # FIXME need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :elastic_mapreduce

    desc 'elastic_mapreduce', 'Launch an ElasticMapReduce ssh session'
    method_option :list, :type => :boolean, :desc => 'List all job flows created in the last 2 days', :default => false
    method_option :jobflow, :aliases => '-j', :desc => 'Elastic MapReduce jobflow ID (Hint: --list)'
    def elastic_mapreduce_exec
      elastic_mapreduce(options.merge(interactive: true))
    end
    default_task :elastic_mapreduce_exec

    no_tasks do
      def log_enabled?
        false
      end
    end

    private

    def before_initialize
      abort 'ElasticMapreduce is not enabled' unless Masamune.configuration.elastic_mapreduce[:enabled]
      if options[:list]
        elastic_mapreduce(options.merge(replace: true))
      end
    end
  end
end
