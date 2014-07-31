# TODO consider if name should specified as option
# TODO consider if csv should just be files
# TODO consider if map should use named parameters
module Masamune::Schema
  class Registry
    include Masamune::HasEnvironment

    attr_accessor :dimensions
    attr_accessor :csv_files
    attr_accessor :maps

    def initialize(environment)
      self.environment = environment
      @dimensions = {}
      @csv_files = Hash.new { |h,k| h[k] = [] }
      @maps = {}
      @options = Hash.new { |h,k| h[k] = [] }
      @extra = []
    end

    def schema(options = {}, &block)
      @options.merge!(options)
      instance_eval &block
    end

    def dimension(options = {}, &block)
      prev_options = @options.dup
      yield if block_given?
      self.dimensions[options[:name].to_sym] ||= Masamune::Schema::Dimension.new(options.merge(@options))
    ensure
      @options = prev_options
    end

    def column(options = {}, &block)
      @options[:columns] << Masamune::Schema::Column.new(options)
    end

    def references(name)
      @options[:references] << dimensions[name.to_sym]
    end

    def row(options)
      attributes = options.delete(:attributes) || {}
      attributes[:values] = options
      @options[:rows] << Masamune::Schema::Row.new(attributes)
    end

    def csv(options, &block)
      prev_options = @options.dup
      yield if block_given?
      csv_files = options.delete(:files)
      filesystem.glob(csv_files) do |file|
        # TODO get local copy of file if remote
        self.csv_files[options[:name].to_sym] << Masamune::Schema::File.new(options.merge(@options).merge(file: file))
      end
    ensure
      @options = prev_options
    end

    def map(name, options = {}, &block)
      prev_options = @options.dup
      @options[:fields] = {}
      yield if block_given?
      self.maps[name.to_sym] ||= Masamune::Schema::Map.new(options.merge(@options))
    ensure
      @options = prev_options
    end

    def field(key, value = nil, &block)
      @options[:fields][key.to_sym] = value
      @options[:fields][key.to_sym] ||= block.to_proc if block_given?
      @options[:fields][key.to_sym] ||= key
    end

    def load(file)
      if file =~ /\.rb\Z/
        instance_eval(::File.read(file))
      else
        @extra << ::File.read(file)
      end
    end

     # TODO construct a partial ordering of dimensions by reference
    def as_psql
      output = []
      dimensions.each do |name, dimension|
        logger.debug("#{name}\n" + dimension.as_psql) if dimension.debug
        output << dimension.as_psql
      end
      @extra.each do |extra|
        output << extra
      end
      output.join("\n")
    end

    def to_psql_file
      Tempfile.new('masamune').tap do |file|
        file.write(as_psql)
        file.close
      end.path
    end
  end
end
