require 'delegate'
require 'forwardable'

module Masamune
  module HasEnvironment
    extend Forwardable

    def environment
      @environment ||= Masamune::Environment.new
    end

    def environment=(environment)
      @environment = environment
    end

    def_delegators :environment, :configure, :configuration, :with_exclusive_lock, :logger, :log_file_name, :filesystem, :filesystem=, :trace, :console, :postgres_helper
  end
end
