require 'date'

module Masamune
  module Thor
    def self.included(thor)
      thor.class_eval do
        include Masamune::Actions::Filesystem

        namespace :masamune
        class_option :debug, :aliases => '-d', :desc => 'Print debugging information', :default => false
        class_option :dryrun, :aliases => '-n', :desc => 'Dryrun', :default => false
        class_option :jobflow, :aliases => '-j', :desc => 'Elastic MapReduce jobflow ID (Hint: elastic-mapreduce --list)', :required => Masamune.configuration.elastic_mapreduce
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

