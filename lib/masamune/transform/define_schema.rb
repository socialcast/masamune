require 'masamune/transform/define_table'
require 'masamune/transform/define_event_view'

module Masamune::Transform
  module DefineSchema
    include DefineTable
    include DefineEventView

    extend ActiveSupport::Concern

    def define_schema(catalog, store_id)
      context = catalog[store_id]
      operators = []

      operators += context.extra(:pre)

      context.dimensions.each do |_, dimension|
        operators << define_table(dimension)
      end

      context.facts.each do |_, fact|
        operators << define_table(fact)
      end

      context.events.each do |_, event|
        operators << define_event_view(event)
      end

      operators += context.extra(:post)

      Operator.new __method__, *operators, source: context
    end
  end
end
