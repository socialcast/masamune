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

module Masamune
  module Transform
    require 'masamune/transform/operator'

    require 'masamune/transform/define_table'
    require 'masamune/transform/define_event_view'
    require 'masamune/transform/define_schema'
    require 'masamune/transform/denormalize_table'

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
