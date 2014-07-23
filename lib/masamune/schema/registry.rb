module Masamune::Schema
  class Registry
    attr_accessor :dimensions

    def initialize
      @dimensions = {}
      @current_dimension = nil
    end

    def schema(&block)
      instance_eval &block
    end

    def dimension(a, &block)
      previous_dimension = @current_dimension
      self.dimensions[a[:name].to_sym] ||= Masamune::Schema::Dimension.new(a)
      @current_dimension = self.dimensions[a[:name].to_sym]
      yield if block_given?
    ensure
      @current_dimension = previous_dimension
    end

    def column(a)
      @current_dimension.columns << Masamune::Schema::Column.new(a)
    end

    def references(a)
      reference = dimensions[a.to_sym]
      @current_dimension.references << reference
    end

    def value(a)
      @current_dimension.values << a
    end

    def load(file)
      instance_eval(File.read(file))
    end

    def to_file
      Tempfile.new('masamune').tap do |file|
        file.write(to_s)
        file.close
      end.path
    end

    def empty?
      dimensions.empty?
    end

    def to_s
      dimensions.values.map(&:to_s).join("\n")
    end
  end
end
