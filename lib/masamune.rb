module Masamune
  require 'masamune/configuration'
  require 'masamune/matcher'
  require 'masamune/data_plan'
  require 'masamune/thor'
  require 'masamune/thor_loader'
  require 'masamune/thor_data'
  require 'masamune/actions/common'
  require 'masamune/actions/hive'
  require 'masamune/actions/s3cmd'
  require 'masamune/actions/streaming'
  require 'masamune/actions/filesystem'
  require 'masamune/actions/dataflow'
  require 'masamune/actions/elastic_mapreduce'
  require 'masamune/filesystem'
  require 'masamune/filesystem/hadoop'
  require 'masamune/filesystem/s3'
  require 'masamune/store'

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
  def_delegators :configuration, :logger, :filesystem, :trace
end
