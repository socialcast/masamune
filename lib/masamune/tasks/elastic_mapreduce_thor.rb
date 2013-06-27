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
    method_option :jobflow, :aliases => '-j', :desc => 'Elastic MapReduce jobflow ID (Hint: -- --list)'
    def elastic_mapreduce_exec
      elastic_mapreduce(options.merge(interactive: true, extra: extra_or_ssh))
    end
    default_task :elastic_mapreduce_exec

    no_tasks do
      def log_enabled?
        false
      end

      def extra_or_ssh
        self.extra.any? ? self.extra : ['--ssh']
      end
    end
  end
end
