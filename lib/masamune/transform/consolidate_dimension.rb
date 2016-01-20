#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
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
        bulk_upsert(target.stage_table(suffix: 'consolidated_reverse'), target.stage_table(suffix: 'consolidated')),
        bulk_upsert(target.stage_table(suffix: 'consolidated_forward'), target.stage_table(suffix: 'consolidated')),
        deduplicate_dimension(target.stage_table(suffix: 'consolidated'), target.stage_table(suffix: 'deduplicated')),
        bulk_upsert(target.stage_table(suffix: 'deduplicated'), target),
        relabel_dimension(target)
    end
  end
end
