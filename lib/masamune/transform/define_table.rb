module Masamune::Transform
  module DefineTable
    extend ActiveSupport::Concern

    def define_table(target, files = [])
      return if target.implicit
      Operator.new(__method__, target: target, files: convert_files(files), presenters: { hive: Hive }).tap do |operator|
        logger.debug("#{target.id}\n" + operator.to_s) if target.debug
      end
    end

    private

    def convert_files(files)
      case files
      when Set
        files.to_a
      when Array
        files
      else
        Array.wrap(files)
      end
    end

    class Hive < SimpleDelegator
      def partition_by
        partitions.map { |_, column| "#{column.name} #{column.hql_type}" }.join(', ')
      end
    end
  end
end
