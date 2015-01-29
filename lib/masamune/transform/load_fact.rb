require 'masamune/transform/define_table'
require 'masamune/transform/stage_fact'
require 'masamune/transform/insert_reference_values'
require 'masamune/transform/bulk_upsert'

module Masamune::Transform
  module LoadFact
    include DefineTable
    include StageFact
    include InsertReferenceValues
    include BulkUpsert

    extend ActiveSupport::Concern

    def load_fact(files, source, target, date)
      source = source.as_table(target)
      Operator.new \
        define_table(source, files),
        insert_reference_values(source, target),
        stage_fact(source, target, date)
    end
  end
end
