require 'active_support/concern'
require 'masamune/actions/postgres'

module Masamune::Actions
  module Transform
    extend ActiveSupport::Concern
    extend Forwardable

    include Masamune::Actions::Postgres

    def_delegators :registry, :dimensions, :maps, :files, :facts

    FILE_MODE = 0777 - File.umask

    def load_dimension(source_file, source, target)
      input = File.open(source_file)
      output = Tempfile.new('masamune')
      FileUtils.chmod(FILE_MODE, output.path)

      if source.respond_to?(:map) and map = source.map(to: target)
        result = map.apply(input, output)
      else
        result = input
      end

      transform = Masamune::Transform::LoadDimension.new(output, result, target)
      logger.debug(File.read(output)) if (source.debug || map.debug)
      postgres file: transform.to_psql_file, debug: (source.debug || target.debug || map.debug)
    ensure
      input.close
      output.unlink
    end

    def consolidate_dimension(target)
      transform = Masamune::Transform::ConsolidateDimension.new(target)
      postgres file: transform.to_psql_file, debug: target.debug
    end

    def relabel_dimension(target)
      transform = Masamune::Transform::RelabelDimension.new(target)
      postgres file: transform.to_psql_file, debug: target.debug
    end

    def load_fact(source_files, source, target, date, grain = nil)
      transform = Masamune::Transform::LoadFact.new(source_files, source, target, date, grain)
      postgres file: transform.to_psql_file, debug: (source.debug || target.debug)
    end
  end
end
