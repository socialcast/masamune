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

module Masamune::Transform
  module DefineSchema
    include DefineTable

    extend ActiveSupport::Concern

    def define_schema(catalog, store_id, options = {})
      context = catalog[store_id]
      operators = []

      operators += context.extra(:pre)

      context.dimensions.each do |_, dimension|
        operators << define_table(dimension, [], options[:section])
      end

      context.facts.each do |_, fact|
        operators << define_table(fact, [], options[:section])
        fact.partition_tables(options[:start_date], options[:stop_date]) do |fact_partition_table|
          operators << define_table(fact_partition_table, [], options[:section])
        end
      end

      operators += context.extra(:post)

      Operator.new __method__, *operators, source: context
    end
  end
end
