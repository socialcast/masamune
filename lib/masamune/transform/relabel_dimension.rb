module Masamune::Transform
  module RelabelDimension
    extend ActiveSupport::Concern

    def relabel_dimension(target)
      Operator.new(__method__, target: target, presenters: { postgres: Postgres })
    end

    private

    class Postgres < SimpleDelegator
      def window(*extra)
        (columns.values.select { |column| extra.delete(column.name) || column.natural_key || column.auto_reference }.map(&:name) + extra).uniq
      end
    end
  end
end
