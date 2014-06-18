require 'date'
require 'thor'
require 'active_support/concern'

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
    ]

    module ExtraArguments
      def parse_extra(argv)
        if i = argv.index('--')
          if i > 0
            [argv[0 .. i-1], argv[i+1..-1]]
          else
            [[], argv[i+1..-1]]
          end
        else
          [argv, []]
        end
      end
    end

    module RescueLogger
      def start(*a)
        super
      rescue => e
        Masamune.logger.error(e.to_s)
        raise e
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
        class_option :help, :type => :boolean, :aliases => '-h', :desc => 'Show help', :default => false
        class_option :quiet, :type => :boolean, :aliases => '-q', :desc => 'Suppress all output', :default => false
        class_option :verbose, :type => :boolean, :aliases => '-v', :desc => 'Print command execution information', :default => false
        class_option :debug, :type => :boolean, :aliases => '-d', :desc => 'Print debugging information', :default => false
        class_option :no_op, :type => :boolean, :desc => 'Do not execute commands that modify state', :default => false
        class_option :dry_run, :type => :boolean, :aliases => '-n', :desc => 'Combination of --no-op and --verbose', :default => false
        class_option :config, :desc => 'Configuration file'
        class_option :version, :desc => 'Print version and exit', :type => :boolean
        class_option :'--', :desc => 'Extra pass through arguments'
        def initialize(_args=[], _options={}, _config={})
          self.environment.parent = self
          self.filesystem.environment = self
          self.current_namespace = self.class.namespace unless self.class.namespace == 'masamune'
          self.current_task_name = _config[:current_command].name
          self.current_command_name = current_namespace ? current_namespace + ':' + current_task_name : current_task_name

          if _options.is_a?(Array)
            _options, self.extra = self.class.parse_extra(_options)
          end

          super _args, _options, _config

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
              raise $! if options[:debug]
              raise ::Thor::MalformattedArgumentError, "Could not load file provided for '--config'"
            end

            config.quiet    = options[:quiet]
            config.verbose  = options[:verbose] || options[:dry_run]
            config.debug    = options[:debug]
            config.no_op    = options[:no_op] || options[:dry_run]
            config.dry_run  = options[:dry_run]

            if options[:version]
              puts environment.version
              puts options if options[:verbose]
              puts config.to_s if options[:verbose]
              exit
            end
          end

          after_initialize_invoke(options.symbolize_keys)
        end

        no_tasks do
          def param(key)
            environment.configuration.params[key]
          end

          def top_level?
            self.current_command_name == ARGV.first
          end
        end

        private

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

