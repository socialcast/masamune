module Masamune::Transform
  module BulkUpsert
    extend ActiveSupport::Concern

    def bulk_upsert(source, target)
      Operator.new(__method__, source: source, target: target)
    end
  end
end
