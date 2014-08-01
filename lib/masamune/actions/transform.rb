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
      logger.debug(transform.output.to_s) if map.debug
      postgres file: transform.to_psql_file, debug: map.debug
    end
  end
end
