require 'active_support/concern'
require 'masamune/actions/postgres'

module Masamune::Actions
  module Transform
    extend ActiveSupport::Concern
    extend Forwardable

    include Masamune::Actions::Postgres

    def_delegators :registry, :dimensions, :maps, :files

    def load_dimension(file, source, target, map)
      transform = Masamune::Transform::LoadDimension.new(File.open(file), source, target, map)
      transform.run
      logger.debug(transform.output.to_s) if (source.debug || map.debug)
      postgres file: transform.to_psql_file, debug: (source.debug || target.debug || map.debug)
    end

    def consolidate_dimension(target)
      transform = Masamune::Transform::ConsolidateDimension.new(target)
      postgres file: transform.to_psql_file, debug: target.debug
    end
  end
end
