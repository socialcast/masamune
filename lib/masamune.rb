module Masamune
  require 'masamune/environment'
  require 'masamune/has_environment'
  require 'masamune/io'
  require 'masamune/template'
  require 'masamune/commands'
  require 'masamune/accumulate'
  require 'masamune/last_element'
  require 'masamune/actions'
  require 'masamune/helpers'
  require 'masamune/configuration'
  require 'masamune/data_plan'
  require 'masamune/thor'
  require 'masamune/thor_loader'
  require 'masamune/filesystem'
  require 'masamune/cached_filesystem'
  require 'masamune/method_logger'
  require 'masamune/after_initialize_callbacks'
  require 'masamune/schema'
  require 'masamune/transform'
  require 'masamune/topological_hash'

  extend self
  extend Masamune::HasEnvironment

  def load(config_file, &block)
    Masamune::Environment.new.tap do |env|
      env.configure do |config|
        config.load(config_file)
      end
      env.catalog.instance_eval &block
    end
  end
end
