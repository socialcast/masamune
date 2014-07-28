module Masamune::Schema
  class Registry
    include Masamune::HasEnvironment

    attr_accessor :dimensions

    def initialize(environment)
      self.environment = environment
      @dimensions = {}
      @options = Hash.new { |h,k| h[k] = [] }
      @extra = []
    end

    def schema(&block)
      instance_eval &block
    end

    def dimension(a, &block)
      prev_options = @options.dup
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

    def row(a)
      attributes = a.delete(:attributes) || {}
      attributes[:values] = a
      @options[:rows] << Masamune::Schema::Row.new(attributes)
    end

    def load(file)
      if file =~ /\.rb\Z/
        instance_eval(File.read(file))
      else
        @extra << File.read(file)
      end
    end

    def to_file
      Tempfile.new('masamune').tap do |file|
        file.write(to_s)
        file.close
      end.path
    end

    def to_s
      # TODO construct a partial ordering of dimensions by reference
      (dimensions.values.map(&:to_s) + @extra).join("\n")
    end
  end
end
