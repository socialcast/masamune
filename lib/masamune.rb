require 'thread'

module Masamune
  require 'masamune/accumulate'
  require 'masamune/configuration'
  require 'masamune/data_plan'
  require 'masamune/data_plan_rule'
  require 'masamune/data_plan_elem'
  require 'masamune/thor'
  require 'masamune/thor_loader'
  require 'masamune/thor_data'
  require 'masamune/actions/hive'
  require 'masamune/actions/s3cmd'
  require 'masamune/actions/streaming'
  require 'masamune/actions/filesystem'
  require 'masamune/actions/data_flow'
  require 'masamune/actions/elastic_mapreduce'
  require 'masamune/filesystem'
  require 'masamune/cached_filesystem'
  require 'masamune/store'
  require 'masamune/method_logger'
  require 'masamune/proxy_delegate'

  require 'masamune/commands/line_formatter'

  class Client
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

  def_delegators :default_client, :configure, :configuration
  def_delegators :configuration, :logger, :filesystem
  # TODO encapsulate in CLI
  def_delegators :configuration, :trace, :print
end
