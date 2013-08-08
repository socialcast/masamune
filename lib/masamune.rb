require 'thread'

module Masamune
  require 'masamune/io'
  require 'masamune/commands'
  require 'masamune/accumulate'
  require 'masamune/actions'
  require 'masamune/configuration'
  require 'masamune/data_plan'
  require 'masamune/data_plan_rule'
  require 'masamune/data_plan_elem'
  require 'masamune/data_plan_set'
  require 'masamune/data_plan_builder'
  require 'masamune/thor'
  require 'masamune/thor_loader'
  require 'masamune/thor_data'
  require 'masamune/filesystem'
  require 'masamune/cached_filesystem'
  require 'masamune/method_logger'
  require 'masamune/proxy_delegate'

  class Client
    attr_accessor :context

    def configure
      yield configuration
    end

    def configuration
      @configuration ||= Masamune::Configuration.new(self)
    end

    def mutex
      @mutex ||= Mutex.new
    end
  end

  extend Forwardable
  extend self

  def default_client
    @default_client ||= Client.new
  end

  def default_config_file
    @default_config_file ||= File.join(File.expand_path('../../', __FILE__), 'conf', 'masamune.yml.erb')
  end

  def client
    @client || default_client
  end

  def client=(client)
    @client = client
  end

  def_delegators :client, :configure, :configuration
  def_delegators :configuration, :logger, :filesystem
  # TODO encapsulate in CLI
  def_delegators :configuration, :trace, :print
end
