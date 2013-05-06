module Fixpoint
  require 'fixpoint/configuration'
  require 'fixpoint/matcher'
  require 'fixpoint/data_plan'
  require 'fixpoint/thor'
  require 'fixpoint/thor_loader'
  require 'fixpoint/actions/common'
  require 'fixpoint/actions/hive'
  require 'fixpoint/actions/streaming'
  require 'fixpoint/actions/filesystem'
  require 'fixpoint/actions/dataflow'
  require 'fixpoint/filesystem'
  require 'fixpoint/filesystem/hadoop'

  class Client
    def configure
      yield configuration
    end

    def configuration
      @configuration ||= Fixpoint::Configuration.new
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
