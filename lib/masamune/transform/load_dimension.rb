require 'masamune/transform/define_table'
require 'masamune/transform/stage_dimension'
require 'masamune/transform/insert_reference_values'
require 'masamune/transform/bulk_upsert'

module Masamune::Transform
  module LoadDimension
    include DefineTable
    include StageDimension
    include InsertReferenceValues
    include BulkUpsert

    extend ActiveSupport::Concern

    def load_dimension(file, source, target)
      target = target.type == :four ? target.ledger_table : target
      source = source.as_table(target)
      Operator.new \
        define_table(source, file),
        stage_dimension(source, target),
        insert_reference_values(source, target),
        bulk_upsert(target.stage_table, target)
    end
  end
end
