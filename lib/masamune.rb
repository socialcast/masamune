module Masamune
  require 'masamune/context'
  require 'masamune/has_context'
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
  require 'masamune/after_initialize_callbacks'

  extend self
  extend Masamune::HasContext

  def default_config_file
    @default_config_file ||= File.join(File.expand_path('../../', __FILE__), 'config', 'masamune.yml.erb')
  end
end
