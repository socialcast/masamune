module Masamune::Transform
  module DefineTable
    extend ActiveSupport::Concern

    def define_table(target, files = [])
      Operator.new(__method__, target: target, files: Array.wrap(files)).tap do |operator|
        logger.debug("#{target.id}\n" + operator) if target.debug
      end
    end
  end
end
