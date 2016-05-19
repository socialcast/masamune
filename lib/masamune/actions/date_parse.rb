#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'chronic'

module Masamune::Actions
  module DateParse
    extend ActiveSupport::Concern

    def parse_datetime_type(key)
      value = options[key]
      Chronic.parse(value).tap do |datetime_value|
        logger.debug("Using '#{datetime_value}' for --#{key}") if value != datetime_value
      end || raise(Thor::MalformattedArgumentError, "Expected date time value for '--#{key}'; got #{value}")
    end

    included do |base|
      base.class_eval do
        attr_accessor :start_value
        attr_accessor :stop_value
        attr_accessor :exact_value

        class_option :start, aliases: '-a', desc: 'Start time'
        class_option :stop, aliases: '-b', desc: 'Stop time'
        class_option :at, desc: 'Exact time'

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
