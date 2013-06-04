require 'masamune'

module Masamune::Tasks
  class ElasticMapreduceThor < Thor
    include Masamune::Thor
    include Masamune::Actions::ElasticMapreduce

    namespace :masamune
    desc 'elastic_mapreduce', 'Launch an ElasticMapReduce ssh session'
    method_option :jobflow, :aliases => '-j', :desc => 'EMR jobflow ID'
    def elastic_mapreduce_exec
      elastic_mapreduce(options)
    end
  end
end
