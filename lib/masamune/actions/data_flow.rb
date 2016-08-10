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

require 'masamune/actions/date_parse'

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

    def reset_module!
      ClassMethods.reset_module!
    end
    module_function :reset_module!

    private

    # TODO: sources from file or input array
    def parse_file_type(key)
      return Set.new unless key
      (value = options[key]) || (return Set.new)
      File.exist?(value) || raise(Thor::MalformattedArgumentError, "Expected file value for '--#{key}'; got #{value}")
      Set.new File.read(value).split(/\s+/)
    end

    def prepare_and_execute(options = {})
      raise Thor::RequiredArgumentMissingError, "No value provided for required options '--start' or '--at'" unless options[:start] || options[:at] || options[:sources] || options[:targets]
      raise Thor::MalformattedArgumentError, "Cannot specify both option '--sources' and option '--targets'" if options[:sources] && options[:targets]

      desired_sources = Masamune::DataPlan::Set.new current_command_name, parse_file_type(:sources)
      desired_targets = Masamune::DataPlan::Set.new current_command_name, parse_file_type(:targets)

      if start_time && stop_time
        desired_targets.merge engine.targets_for_date_range(current_command_name, start_time, stop_time)
      end

      engine.prepare(current_command_name, options.merge(sources: desired_sources, targets: desired_targets))
      engine.execute(current_command_name, options)
    end

    included do |base|
      base.extend ClassMethods
      base.class_eval do
        class_option :sources, desc: 'File of data sources to process'
        class_option :targets, desc: 'File of data targets to process'
        class_option :resolve, type: :boolean, desc: 'Recursively resolve data dependencies', default: true
      end

      base.after_initialize(:final) do |thor, options|
        thor.engine.environment = thor.environment
        thor.engine.filesystem.environment = thor.environment
        thor.environment.with_process_lock(:data_flow_after_initialize) do
          thor.send(:prepare_and_execute, options)
        end
        exit 0 if thor.top_level?
      end if defined?(base.after_initialize)
    end

    # rubocop:disable Style/ClassVars
    module ClassMethods
      def skip
        initialize_module!
        @@namespaces << namespace
        @@sources << { skip: true }
        @@targets << { skip: true }
      end

      def source(source_options = {})
        initialize_module!
        @@namespaces << namespace
        @@sources << source_options
      end

      def target(target_options = {})
        initialize_module!
        @@targets << target_options
      end

      def create_command(*a)
        initialize_module!
        super.tap do
          @@commands += a
        end
      end

      def engine
        @@engine ||= Masamune::DataPlan::Builder.instance.build(@@namespaces, @@commands, @@sources, @@targets)
      end

      private

      def reset_module!
        @@namespaces = []
        @@targets = []
        @@sources = []
        @@commands = []
        @@engine = nil
      end
      module_function :reset_module!

      def initialize_module!
        @@namespaces ||= []
        @@targets ||= []
        @@sources ||= []
        @@commands ||= []
      end

      # If internal call to Thor::Base.start fails, exit
      def exit_on_failure?
        true
      end
    end
    # rubocop:enable Style/ClassVars
  end
end
