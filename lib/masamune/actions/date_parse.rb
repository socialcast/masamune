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
        attr_accessor :start_value
        attr_accessor :stop_value
        attr_accessor :exact_value

        class_option :start, :aliases => '-a', :desc => 'Start time'
        class_option :stop, :aliases => '-b', :desc => 'Stop time'
        class_option :at, :desc => 'Exact time'

        no_tasks do
          def start_date
            (start_value || exact_value).try(:to_date)
          end

          def start_time
            (start_value || exact_value).try(:to_time)
          end

          def stop_date
            (stop_value || exact_value).try(:to_date)
          end

          def stop_time
            (stop_value || exact_value).try(:to_time)
          end
        end
      end

      base.after_initialize(:latest) do |thor, options|
        thor.start_value = thor.parse_datetime_type(:start) if options[:start]
        thor.exact_value = thor.parse_datetime_type(:at)    if options[:at]
        thor.stop_value  = thor.parse_datetime_type(:stop)  if options[:stop]
      end
    end
  end
end
