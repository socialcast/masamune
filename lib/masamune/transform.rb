module Masamune
  module Transform
    require 'masamune/transform/operator'

    require 'masamune/transform/define_table'
    require 'masamune/transform/define_event_view'
    require 'masamune/transform/define_schema'

    require 'masamune/transform/bulk_upsert'
    require 'masamune/transform/insert_reference_values'

    require 'masamune/transform/stage_dimension'
    require 'masamune/transform/stage_fact'

    require 'masamune/transform/load_dimension'
    require 'masamune/transform/load_fact'

    require 'masamune/transform/snapshot_dimension'
    require 'masamune/transform/deduplicate_dimension'
    require 'masamune/transform/consolidate_dimension'
    require 'masamune/transform/relabel_dimension'

    require 'masamune/transform/rollup_fact'
  end
end
