require 'masamune/has_environment'
require 'masamune/actions/hive'

module Masamune::Helpers
  class Hive
    include Masamune::HasEnvironment
    include Masamune::Actions::Hive

    def initialize(environment)
      self.environment = environment
    end

    def drop_partition(table, partition)
      hive(exec: "ALTER TABLE #{table} DROP PARTITION (#{partition});", fail_fast: true).success?
    end
  end
end
