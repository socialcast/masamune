require 'date'

module Masamune
  module Thor
    def self.included(thor)
      thor.class_eval do
        include Masamune::Actions::Filesystem
        include Masamune::Actions::Dataflow

        namespace :masamune
        class_option :debug, :aliases => '-d', :desc => 'Print debugging information', :default => false
        class_option :start, :aliases => '-a', :desc => 'Start time', :default => nil
        class_option :stop, :aliases => '-b', :desc => 'Stop time', :default => Date.today.to_s, :required => true

        def initialize(*a)
          super
          Masamune.configure do |config|
            config.debug = options[:debug]
          end
        end
      end
    end
  end
end

