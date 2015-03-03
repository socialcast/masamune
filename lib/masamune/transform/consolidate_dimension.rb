require 'masamune/transform/define_table'
require 'masamune/transform/snapshot_dimension'
require 'masamune/transform/bulk_upsert'
require 'masamune/transform/deduplicate_dimension'
require 'masamune/transform/relabel_dimension'

module Masamune::Transform
  module ConsolidateDimension
    include DefineTable
    include BulkUpsert
    include SnapshotDimension
    include DeduplicateDimension
    include RelabelDimension

    extend ActiveSupport::Concern

    def consolidate_dimension(target)
      Operator.new \
        define_table(target.stage_table(suffix: 'consolidated_forward')),
        define_table(target.stage_table(suffix: 'consolidated_reverse')),
        define_table(target.stage_table(suffix: 'consolidated')),
        define_table(target.stage_table(suffix: 'deduplicated')),
        snapshot_dimension(target.ledger_table, target.stage_table(suffix: 'consolidated_forward'), 'ASC'),
        snapshot_dimension(target.ledger_table, target.stage_table(suffix: 'consolidated_reverse'), 'DESC'),
        bulk_upsert(target.stage_table(suffix: 'consolidated_forward'), target.stage_table(suffix: 'consolidated')),
        bulk_upsert(target.stage_table(suffix: 'consolidated_reverse'), target.stage_table(suffix: 'consolidated')),
        deduplicate_dimension(target.stage_table(suffix: 'consolidated'), target.stage_table(suffix: 'deduplicated')),
        bulk_upsert(target.stage_table(suffix: 'deduplicated'), target),
        relabel_dimension(target)
    end
  end
end
