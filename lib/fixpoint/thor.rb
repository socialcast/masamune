require 'date'

module Fixpoint
  module Thor
    def self.included(thor)
      thor.class_eval do
        include Fixpoint::Actions::Filesystem
        include Fixpoint::Actions::Dataflow

        namespace :fixpoint
        class_option :debug, :aliases => '-d', :desc => 'Print debugging information', :default => false
        class_option :start, :aliases => '-a', :desc => 'Start time', :default => nil
        class_option :stop, :aliases => '-b', :desc => 'Stop time', :default => Date.today.to_s, :required => true

        def initialize(*a)
          super
          Fixpoint.configure do |config|
            config.debug = options[:debug]
          end
        end
      end
    end
  end
end

