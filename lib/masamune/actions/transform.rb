require 'active_support/concern'
require 'masamune/actions/postgres'

module Masamune::Actions
  module Transform
    extend ActiveSupport::Concern
    extend Forwardable

    include Masamune::Actions::Postgres

    def_delegators :registry, :dimensions, :maps, :csv_files

    def load_dimension(source, target, map)
      transform = Masamune::Transform::LoadDimension.new(source, target, map)
      postgres file: transform.to_psql_file, debug: map.debug
    end
  end
end
