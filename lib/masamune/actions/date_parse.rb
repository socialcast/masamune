require 'chronic'
require 'active_support/concern'

module Masamune::Actions
  module DateParse
    extend ActiveSupport::Concern

    def parse_datetime_type(key)
      value = options[key]
      Chronic.parse(value).tap do |datetime_value|
        console("Using '#{datetime_value}' for --#{key}") if value != datetime_value
      end or raise Thor::MalformattedArgumentError, "Expected date time value for '--#{key}'; got #{value}"
    end

    private

    included do |base|
      base.class_eval do
        attr_accessor :start_datetime
        attr_accessor :stop_datetime

        class_option :start, :aliases => '-a', :desc => 'Start time', :default => nil
        class_option :stop, :aliases => '-b', :desc => 'Stop time', :default => Date.today.to_s

        private

        def start_date
          start_datetime.to_date
        end

        def stop_date
          stop_datetime.to_date
        end
      end

      base.after_initialize(:latest) do |thor, options|
        thor.start_datetime = thor.parse_datetime_type(:start) if options[:start]
        thor.stop_datetime = thor.parse_datetime_type(:stop) if options[:stop]
      end
    end
  end
end
