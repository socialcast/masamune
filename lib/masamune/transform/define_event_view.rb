require 'masamune/transform/presenter'

module Masamune::Transform
  module DefineEventView
    extend ActiveSupport::Concern

    def define_event_view(target)
      Operator.new(__method__, target: target, presenters: { hql: Hive }).tap do |operator|
        logger.debug("#{target.id}\n" + operator.to_s) if target.debug
      end
    end

    private

    class Hive < Presenter
      def view_name
        "#{id}_events"
      end

      def view_columns
        unreserved_columns.map do |_, column|
          column.name
        end
      end

      def view_values
        unreserved_columns.map do |_, column|
          case column.type
          when :json
            # NOTE could just use split "\t" to parse tsv output
            %Q{CONCAT('"', REGEXP_REPLACE(#{column.name}, '"', '""'),  '"') AS #{column.name}}
          else
            column.name
          end
        end
      end

    end
  end
end
