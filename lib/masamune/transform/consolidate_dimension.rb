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
        define_table(target.stage_table('consolidated_forward')),
        define_table(target.stage_table('consolidated_reverse')),
        define_table(target.stage_table('consolidated')),
        define_table(target.stage_table('deduplicated')),
        snapshot_dimension(target.ledger_table, target.stage_table('consolidated_forward'), 'ASC'),
        snapshot_dimension(target.ledger_table, target.stage_table('consolidated_reverse'), 'DESC'),
        bulk_upsert(target.stage_table('consolidated_forward'), target.stage_table('consolidated')),
        bulk_upsert(target.stage_table('consolidated_reverse'), target.stage_table('consolidated')),
        deduplicate_dimension(target.stage_table('consolidated'), target.stage_table('deduplicated')),
        bulk_upsert(target.stage_table('deduplicated'), target),
        relabel_dimension(target)
    end
  end
end
