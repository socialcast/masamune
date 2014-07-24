module Masamune::Schema
  class Registry
    attr_accessor :dimensions

    def initialize
      @dimensions = {}
      @options = Hash.new { |h,k| h[k] = [] }
    end

    def schema(&block)
      instance_eval &block
    end

    def dimension(a, &block)
      prev_options = @options
      yield if block_given?
      self.dimensions[a[:name].to_sym] ||= Masamune::Schema::Dimension.new(a.merge(@options))
    ensure
      @options = prev_options
    end

    def column(a)
      @options[:columns] << Masamune::Schema::Column.new(a)
    end

    def references(a)
      @options[:references] << dimensions[a.to_sym]
    end

    def value(a)
      @options[:values] << a
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
