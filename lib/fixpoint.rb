module Fixpoint
  require 'fixpoint/configuration'
  require 'fixpoint/matcher'
  require 'fixpoint/data_plan'
  require 'fixpoint/filesystem'
  require 'fixpoint/filesystem/hadoop'
  require 'fixpoint/actions/common'
  require 'fixpoint/actions/hive'

  class Client
    def configure
      yield @configuration
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
end
