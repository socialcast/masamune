module Masamune
  require 'masamune/environment'
  require 'masamune/has_environment'
  require 'masamune/io'
  require 'masamune/template'
  require 'masamune/commands'
  require 'masamune/accumulate'
  require 'masamune/actions'
  require 'masamune/helpers'
  require 'masamune/configuration'
  require 'masamune/data_plan'
  require 'masamune/data_plan_rule'
  require 'masamune/data_plan_elem'
  require 'masamune/data_plan_set'
  require 'masamune/data_plan_builder'
  require 'masamune/thor'
  require 'masamune/thor_loader'
  require 'masamune/filesystem'
  require 'masamune/cached_filesystem'
  require 'masamune/method_logger'
  require 'masamune/proxy_delegate'
  require 'masamune/after_initialize_callbacks'

  extend self
  extend Masamune::HasEnvironment
end
