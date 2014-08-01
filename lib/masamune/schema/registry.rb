module Masamune::Schema
  class Registry
    include Masamune::HasEnvironment

    attr_accessor :dimensions
    attr_accessor :files
    attr_accessor :maps

    def initialize(environment)
      self.environment = environment
      @dimensions = {}
      @files = Hash.new { |h,k| h[k] = [] }
      @maps = {}
      @options = Hash.new { |h,k| h[k] = [] }
      @extra = []
    end

    def schema(options = {}, &block)
      @options.merge!(options)
      instance_eval &block
    end

    def dimension(name, options = {}, &block)
      prev_options = @options.dup
      yield if block_given?
      self.dimensions[name.to_sym] ||= Masamune::Schema::Dimension.new(options.merge(@options).merge(name: name))
    ensure
      @options = prev_options
    end

    def column(name, options = {}, &block)
      @options[:columns] << Masamune::Schema::Column.new(options.merge(name: name))
    end

    def references(name)
      @options[:references] << dimensions[name.to_sym]
    end

    def row(options)
      attributes = options.delete(:attributes) || {}
      attributes[:values] = options
      @options[:rows] << Masamune::Schema::Row.new(attributes)
    end

    def file(name, options, &block)
      prev_options = @options.dup
      yield if block_given?
      files = options.delete(:files)
      filesystem.glob(files) do |file|
        # TODO get local copy of file if remote
        self.files[name.to_sym] << Masamune::Schema::File.new(options.merge(@options).merge(name: name, file: file))
      end
    ensure
      @options = prev_options
    end

    def map(name, options = {}, &block)
      prev_options = @options.dup
      @options[:fields] = {}
      yield if block_given?
      self.maps[name.to_sym] ||= Masamune::Schema::Map.new(options.merge(@options).merge(name: name))
    ensure
      @options = prev_options
    end

    def field(name, value = nil, &block)
      @options[:fields][name.to_sym] = value
      @options[:fields][name.to_sym] ||= block.to_proc if block_given?
      @options[:fields][name.to_sym] ||= name
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
