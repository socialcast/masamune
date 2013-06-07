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
      abort 'ElasticMapreduce not configured' unless Masamune.configuration.elastic_mapreduce
      abort %q(No value provided for required options '--jobflow (Hint: --list)') unless options[:list] || options[:jobflow]
      elastic_mapreduce(options)
    end
    default_task :elastic_mapreduce_exec
  end
end
