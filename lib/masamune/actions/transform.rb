#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'active_support/concern'

require 'masamune/actions/postgres'

require 'masamune/transform/load_dimension'
require 'masamune/transform/consolidate_dimension'
require 'masamune/transform/relabel_dimension'
require 'masamune/transform/load_fact'
require 'masamune/transform/rollup_fact'

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
      extend Masamune::Transform::RollupFact
    end

    FILE_MODE = 0777 - File.umask

    def load_dimension(source_files, source, target)
      optional_apply_map(source_files, source, target) do |intermediate_files, intermediate|
        transform = Wrapper.load_dimension(intermediate_files, intermediate, target)
        postgres file: transform.to_file, debug: (source.debug || target.debug || intermediate.debug)
      end
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
      optional_apply_map(source_files, source, target) do |intermediate_files, intermediate|
        transform = Wrapper.load_fact(intermediate_files, intermediate, target, date)
        postgres file: transform.to_file, debug: (source.debug || target.debug || intermediate.debug)
      end
    end

    def rollup_fact(source, target, date)
      transform = Wrapper.rollup_fact(source, target, date)
      postgres file: transform.to_file, debug: (source.debug || target.debug)
    end

    private

    def optional_apply_map(source_files, source, target, &block)
      if source.respond_to?(:map) and map = source.map(to: target)
        apply_map(map, source_files, source, target, &block)
      else
        yield source_files, source
      end
    end

    def apply_map(map, source_files, source, target)
      Tempfile.open('masamune') do |output|
        begin
          FileUtils.chmod(FILE_MODE, output.path)
          result = map.apply(source_files, output)
          result.debug = map.debug
          logger.debug(File.read(output)) if (source.debug || result.debug)
          yield output, result
        ensure
          output.unlink
        end
      end
    end
  end
end
