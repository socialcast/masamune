module Masamune
  require 'masamune/configuration'
  require 'masamune/matcher'
  require 'masamune/data_plan'
  require 'masamune/thor'
  require 'masamune/thor_loader'
  require 'masamune/actions/common'
  require 'masamune/actions/hive'
  require 'masamune/actions/streaming'
  require 'masamune/actions/filesystem'
  require 'masamune/actions/dataflow'
  require 'masamune/filesystem'
  require 'masamune/filesystem/hadoop'

  class Client
    def configure
      yield configuration
    end

    def configuration
      @configuration ||= Masamune::Configuration.new
    end
  end

  extend Forwardable
  extend self

  def default_client
    @default_client ||= Client.new
  end

  def_delegators :default_client, :configure, :configuration
  def_delegators :configuration, :logger, :filesystem
end
