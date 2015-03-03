#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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
require 'active_support/concern'

module Masamune::Actions
  module DataFlow
    extend ActiveSupport::Concern

    include Masamune::Actions::DateParse

    def engine
      self.class.engine
    end

    def targets
      engine.targets(current_command_name)
    end

    def sources
      engine.sources(current_command_name)
    end

    def target
      targets.first
    end

    def source
      sources.first
    end

    # TODO sources from file or input array
    def parse_file_type(key)
      return Set.new unless key
      value = options[key] or return Set.new
      File.exists?(value) or raise Thor::MalformattedArgumentError, "Expected file value for '--#{key}'; got #{value}"
      Set.new File.read(value).split(/\s+/)
    end

    private

    included do |base|
      base.extend ClassMethods
      base.class_eval do
        class_option :sources, :desc => 'File of data sources to process'
        class_option :targets, :desc => 'File of data targets to process'
        class_option :resolve, :type => :boolean, :desc => 'Recursively resolve data dependencies', :default => true
      end

      base.after_initialize(:final) do |thor, options|
        # Only execute this block if DataPlan::Engine is not currently executing
        next if thor.engine.executing?
        thor.engine.environment = thor.environment
        thor.engine.filesystem.environment = thor.environment

        raise Thor::RequiredArgumentMissingError, "No value provided for required options '--start' or '--at'" unless options[:start] || options[:at] || options[:sources] || options[:targets]
        raise Thor::MalformattedArgumentError, "Cannot specify both option '--sources' and option '--targets'" if options[:sources] && options[:targets]

        desired_sources = Masamune::DataPlan::Set.new thor.current_command_name, thor.parse_file_type(:sources)
        desired_targets = Masamune::DataPlan::Set.new thor.current_command_name, thor.parse_file_type(:targets)

        if thor.start_time && thor.stop_time
          desired_targets.merge thor.engine.targets_for_date_range(thor.current_command_name, thor.start_time, thor.stop_time)
        end

        thor.engine.prepare(thor.current_command_name, options.merge(sources: desired_sources, targets: desired_targets))
        thor.engine.execute(thor.current_command_name, options)
        exit 0 if thor.top_level?
      end if defined?(base.after_initialize)
    end

    module ClassMethods
      def skip
        @@namespaces ||= []
        @@namespaces << namespace
        @@sources ||= []
        @@sources << {skip: true}
        @@targets ||= []
        @@targets << {skip: true}
      end

      def source(source_options = {})
        @@namespaces ||= []
        @@namespaces << namespace
        @@sources ||= []
        @@sources << source_options
      end

      def target(target_options = {})
        @@targets ||= []
        @@targets << target_options
      end

      def create_command(*a)
        super.tap do
          @@commands ||= []
          @@commands += a
        end
      end

      def engine
        @@engine ||= Masamune::DataPlan::Builder.instance.build(@@namespaces, @@commands, @@sources, @@targets)
      end

      private

      # If internal call to Thor::Base.start fails, exit
      def exit_on_failure?
        true
      end
    end
  end
end
