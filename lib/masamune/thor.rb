require 'date'
require 'thor'

module Masamune
  module Thor
    SYSTEM_CONFIG_FILES = [
      '/etc/masamune/config.yml',
      '/etc/masamune/config.yml.erb',
      "#{ENV['HOME']}/.masamune/config.yml"
    ]

    module ExtraArguments
      def parse_extra(argv)
        if i = argv.index('--')
          [argv[0 .. i-1], argv[i+1..-1]]
        else
          [argv, []]
        end
      end
    end

    def self.included(thor)
      thor.extend ExtraArguments
      thor.class_eval do
        include Masamune::Actions::Filesystem
        include Masamune::Actions::ElasticMapreduce
        attr_accessor :extra

        namespace :masamune
        class_option :help, :type => :boolean, :aliases => '-h', :desc => 'Show help', :default => false
        class_option :quiet, :type => :boolean, :aliases => '-q', :desc => 'Suppress all output', :default => false
        class_option :verbose, :type => :boolean, :aliases => '-v', :desc => 'Print command execution information', :default => false
        class_option :debug, :type => :boolean, :aliases => '-d', :desc => 'Print debugging information', :default => false
        class_option :no_op, :type => :boolean, :desc => 'Do not execute commands that modify state', :default => false
        class_option :dry_run, :type => :boolean, :aliases => '-n', :desc => 'Combination of --no-op and --verbose', :default => false
        class_option :jobflow, :aliases => '-j', :desc => 'Elastic MapReduce jobflow ID (Hint: elastic-mapreduce --list)'
        class_option :config, :desc => 'Configuration file', :default => Masamune.default_config_file
        class_option :version, :desc => 'Print version and exit'
        class_option :'--', :desc => 'Extra pass through arguments'
        def initialize(_args=[], _options={}, _config={})
          if _options.is_a?(Array)
            _options, self.extra = self.class.parse_extra(_options)
          end

          super _args, _options, _config

          if display_help?
            display_help
            exit
          end

          Masamune.configure do |config|
            config.client.context = self

            if options[:config]
              config.load(options[:config])
            elsif system_config_file = config.filesystem.resolve_file(SYSTEM_CONFIG_FILES)
              config.load(system_config_file)
            end

            config.quiet    = options[:quiet]
            config.verbose  = options[:verbose] || options[:dry_run]
            config.debug    = options[:debug]
            config.no_op    = options[:no_op] || options[:dry_run]
            config.dry_run  = options[:dry_run]
            config.jobflow  = options[:jobflow]

            if options[:version]
              puts config.version
              puts options if options[:verbose]
              puts config.to_s if options[:verbose]
              exit
            end
          end

          before_initialize

          if Masamune.configuration.elastic_mapreduce[:enabled]
            raise ::Thor::RequiredArgumentMissingError, "No value provided for required options '--jobflow'" unless options[:jobflow] if self.extra.empty?
            raise ::Thor::RequiredArgumentMissingError, %Q(Value '#{options[:jobflow]}' for '--jobflow' doesn't exist) unless elastic_mapreduce(list: true, jobflow: options[:jobflow], fail_fast: false).success?
          end

          if options[:dry_run]
            raise ::Thor::InvocationError, 'Dry run of hive failed' unless hive(exec: 'show tables;', safe: true, fail_fast: false).success?
          end

          after_initialize
        end

        private

        def before_initialize(*a); end
        def after_initialize(*a); end

        def display_help?
          options[:help] || current_command.name == 'help'
        end

        def display_help
          help
        end
      end

      def current_command
        @_initializer.last[:current_command]
      end
    end
  end
end

