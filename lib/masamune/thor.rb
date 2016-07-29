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

require 'date'
require 'thor'

require 'masamune/has_environment'
require 'masamune/after_initialize_callbacks'

module Masamune
  module Thor
    extend ActiveSupport::Concern

    include Masamune::HasEnvironment
    include Masamune::AfterInitializeCallbacks

    SYSTEM_CONFIG_FILES = [
      '/etc/masamune/config.yml',
      '/etc/masamune/config.yml.erb',
      '/opt/masamune/etc/config.yml',
      '/opt/masamune/etc/config.yml.erb',
      '/opt/etc/masamune/config.yml',
      '/opt/etc/masamune/config.yml.erb',
      "#{ENV['HOME']}/.masamune/config.yml"
    ].freeze

    module ExtraArguments
      def parse_extra(argv)
        i = argv.index('--')
        if i
          if i > 0
            [argv[0..i - 1], argv[i + 1..-1]]
          else
            [[], argv[i + 1..-1]]
          end
        else
          [argv, []]
        end
      end
    end

    module RescueLogger
      def instance=(instance)
        @instance = instance
      end

      def instance
        @instance || Masamune
      end

      def start(*a)
        super
      rescue SignalException => e
        raise e unless %w(SIGHUP SIGTERM).include?(e.to_s)
        instance.logger.debug("Exiting at user request on #{e}")
        exit 0
      rescue ::Thor::MalformattedArgumentError, ::Thor::RequiredArgumentMissingError => e
        raise e
      rescue => e
        instance.logger.error("#{e.message} (#{e.class}) backtrace:")
        e.backtrace.each { |x| instance.logger.error(x) }
        $stderr.puts "For complete debug log see: #{instance.log_file_name}" if instance.log_file_name
        abort e.message
      end
    end

    included do |thor|
      thor.extend ExtraArguments
      thor.extend RescueLogger
      thor.class_eval do
        include Masamune::Actions::Filesystem

        attr_accessor :current_namespace
        attr_accessor :current_task_name
        attr_accessor :current_command_name
        attr_accessor :extra

        namespace :masamune
        class_option :help, type: :boolean, aliases: '-h', desc: 'Show help', default: false
        class_option :quiet, type: :boolean, aliases: '-q', desc: 'Suppress all output', default: false
        class_option :verbose, type: :boolean, aliases: '-v', desc: 'Print command execution information', default: false
        class_option :debug, type: :boolean, aliases: '-d', desc: 'Print debugging information', default: false
        class_option :dry_run, type: :boolean, aliases: '-n', desc: 'Do not execute commands that modify state', default: false
        class_option :config, desc: 'Configuration file'
        class_option :version, desc: 'Print version and exit', type: :boolean
        class_option :lock, desc: 'Optional job lock name', type: :string
        class_option :initialize, aliases: '--init', desc: 'Initialize configured data stores', type: :boolean, default: false
        class_option :'--', desc: 'Extra pass through arguments'
        def initialize(thor_args = [], thor_options = {}, thor_config = {})
          environment.parent = self
          filesystem.environment = self
          self.current_namespace = self.class.namespace unless self.class.namespace == 'masamune'
          self.current_task_name = thor_config[:current_command].try(:name)
          self.current_command_name = current_namespace ? current_namespace + ':' + current_task_name : current_task_name
          self.class.instance = self

          define_current_dir

          if thor_options.is_a?(Array)
            thor_options, self.extra = self.class.parse_extra(thor_options)
          end

          super thor_args, thor_options, thor_config

          if display_help?
            display_help
            exit
          end

          environment.configure do |config|
            config_file = options[:config]
            config_file ||= config.filesystem.resolve_file([config.default_config_file] + SYSTEM_CONFIG_FILES)
            raise ::Thor::RequiredArgumentMissingError, 'Option --config or valid system configuration file required' unless config_file

            begin
              config.load(config_file)
            rescue
              raise $ERROR_INFO if options[:debug]
              raise ::Thor::MalformattedArgumentError, "Could not load file provided for '--config'"
            end

            config.quiet    = options[:quiet]
            config.verbose  = options[:verbose] || options[:dry_run]
            config.debug    = options[:debug]
            config.dry_run  = options[:dry_run]
            config.lock     = options[:lock]

            if options[:version]
              puts environment.version
              exit
            end
          end

          after_initialize_invoke(options.to_hash.symbolize_keys)
        end

        no_tasks do
          def param(key)
            environment.configuration.params[key]
          end

          def top_level?
            current_command_name == ARGV.first
          end

          def invoke_command(command, *args)
            return super if self.class.skip_lock?
            lock_name = qualify_task_name(command.name) + '_command'
            environment.with_exclusive_lock(lock_name) do
              super
            end
          end

          def invoke(name = nil, *args)
            lock_name = qualify_task_name(name) + '_task'
            environment.with_exclusive_lock(lock_name) do
              super
            end
          end

          def qualify_task_name(task_name)
            task, namespace = *task_name.split(':').reverse
            namespace ||= current_namespace
            "#{namespace}:#{task.gsub(/_task\Z/, '')}"
          end

          class << self
            def skip_lock!
              @skip_lock = true
            end

            def skip_lock?
              @skip_lock
            end
          end
        end

        private

        def define_current_dir
          return unless current_task_name
          filesystem.add_path(:current_dir, File.dirname(method(current_task_name).source_location.first))
        end

        def display_help?
          options[:help] || current_task_name == 'help'
        end

        def display_help
          if options[:help]
            help current_task_name
          elsif current_task_name == 'help'
            help args.first || default_and_only_command
          else
            help
          end
        end

        def default_and_only_command
          self.class.default_command if self.class.tasks.count == 1
        end
      end
    end
  end
end
