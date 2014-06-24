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
    class_option :template, :type => :string, :aliases => '-t', :desc => 'Execute named template command'
    class_option :params, :type => :hash, :aliases => '-p', :desc => 'Bind params to named template command', :default => {}
    def elastic_mapreduce_exec
      elastic_mapreduce(options.merge(interactive: true, extra: extra_or_ssh))
    end
    default_task :elastic_mapreduce_exec

    no_tasks do
      after_initialize(:first) do |thor, options|
        begin
          thor.extra += thor.configuration.bind_template(:elastic_mapreduce, options[:template], options[:params]) if options[:template]
        rescue ArgumentError => e
          raise ::Thor::MalformattedArgumentError, e.to_s
        end
      end

      def extra_or_ssh
        self.extra.any? ? self.extra : ['--ssh']
      end
    end
  end
end
