require 'active_support/concern'

require 'masamune/actions/postgres'

require 'masamune/transform/load_dimension'
require 'masamune/transform/consolidate_dimension'
require 'masamune/transform/relabel_dimension'
require 'masamune/transform/load_fact'

module Masamune::Actions
  module Transform
    extend ActiveSupport::Concern
    extend Forwardable

    include Masamune::Actions::Postgres

    # FIXME should eventually be able to include Transform directly instead of through wrapper
    class Wrapper
      extend Masamune::Transform::LoadDimension
      extend Masamune::Transform::ConsolidateDimension
      extend Masamune::Transform::RelabelDimension
      extend Masamune::Transform::LoadFact
    end

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
      logger.debug(File.read(output)) if source.debug

      transform = Wrapper.load_dimension(output, result, target)
      postgres file: transform.to_file, debug: (source.debug || target.debug)
    ensure
      input.close
      output.unlink
    end

    def consolidate_dimension(target)
      transform = Wrapper.consolidate_dimension(target)
      postgres file: transform.to_file, debug: target.debug
    end

    def relabel_dimension(target)
      transform = Wrapper.relabel_dimension(target)
      postgres file: transform.to_file, debug: target.debug
    end

    def load_fact(source_files, source, target, date)
      transform = Wrapper.load_fact(source_files, source, target, date)
      postgres file: transform.to_file, debug: (source.debug || target.debug)
    end
  end
end
