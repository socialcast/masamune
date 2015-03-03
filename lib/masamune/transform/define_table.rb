module Masamune::Transform
  module DefineTable
    extend ActiveSupport::Concern

    def define_table(target, files = [])
      return if target.implicit
      Operator.new(__method__, target: target, files: Masamune::Schema::Map.convert_files(files), presenters: { hive: Hive }).tap do |operator|
        logger.debug("#{target.id}\n" + operator.to_s) if target.debug
      end
    end

    class Hive < SimpleDelegator
      def partition_by
        partitions.map { |_, column| "#{column.name} #{column.hql_type}" }.join(', ')
      end
    end
  end
end
