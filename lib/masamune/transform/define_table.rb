module Masamune::Transform
  module DefineTable
    extend ActiveSupport::Concern

    def define_table(target, files = [])
      Operator.new(__method__, target: target, files: convert_files(files)).tap do |operator|
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
  end
end
