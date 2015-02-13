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

    def convert_file(file)
      if file.respond_to?(:path)
        file.flush if file.respond_to?(:flush)
        file.path
      else
        file
      end
    end

    def convert_files(files)
      case files
      when Set
        files.map { |file| convert_file(file) }.to_a
      when Array
        files.map { |file| convert_file(file) }.to_a
      else
        [convert_file(files)]
      end
    end

    class Hive < SimpleDelegator
      def partition_by
        partitions.map { |_, column| "#{column.name} #{column.hql_type}" }.join(', ')
      end
    end
  end
end
