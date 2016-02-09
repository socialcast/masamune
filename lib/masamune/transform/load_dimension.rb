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

    def load_dimension(files, source, target)
      target = target.type == :four ? target.ledger_table : target
      source = source.stage_table(suffix: 'file', table: target, inherit: false)
      Operator.new \
        define_table(source, files: files),
        insert_reference_values(source, target),
        stage_dimension(source, target),
        bulk_upsert(target.stage_table, target)
    end
  end
end
