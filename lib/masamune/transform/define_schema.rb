require 'masamune/transform/define_table'
require 'masamune/transform/define_event_view'

module Masamune::Transform
  module DefineSchema
    include DefineTable
    include DefineEventView

    extend ActiveSupport::Concern

    def define_schema(registry)
      operators = []

      registry.dimensions.each do |_, dimension|
        operators << define_table(dimension)
      end

      registry.facts.each do |_, fact|
        operators << define_table(fact)
      end

      registry.events.each do |_, event|
        operators << define_event_view(event)
      end

      # TODO need per 'kind' extra- should be handled by multiple registries

      Operator.new __method__, *operators, source: registry
    end
  end
end
