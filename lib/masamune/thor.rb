require 'date'

module Masamune
  module Thor
    def self.included(thor)
      thor.class_eval do
        include Masamune::Actions::Filesystem
        include Masamune::Actions::Dataflow

        namespace :masamune
        class_option :debug, :aliases => '-d', :desc => 'Print debugging information', :default => false
        class_option :start, :aliases => '-a', :desc => 'Start time', :default => nil, :required => true
        class_option :stop, :aliases => '-b', :desc => 'Stop time', :default => Date.today.to_s
        class_option :dryrun, :aliases => '-n', :desc => 'Dryrun', :default => false
        class_option :jobflow, :aliases => '-j', :desc => 'Elastic MapReduce jobflow ID', :long_desc => 'Try elastic-mapreduce --list', :required => Masamune.configuration.elastic_mapreduce
        def initialize(*a)
          super
          Masamune.configure do |config|
            config.debug = options[:debug]
            config.dryrun = options[:dryrun]
            config.jobflow = options[:jobflow]
          end
        end
      end
    end
  end
end

