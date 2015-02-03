require 'active_support/core_ext/hash'

module Masamune::Schema
  class Store
    SUPPORTED_ATTRIBUTES = %(table dimension fact file event)

    attr_accessor :type
    attr_accessor :tables
    attr_accessor :dimensions
    attr_accessor :facts
    attr_accessor :files
    attr_accessor :events
    attr_accessor :references

    def initialize(type)
      @type       = type
      @tables     = {}.with_indifferent_access
      @dimensions = {}.with_indifferent_access
      @facts      = {}.with_indifferent_access
      @files      = {}.with_indifferent_access
      @events     = {}.with_indifferent_access
      @references = {}.with_indifferent_access
      @extra      = []
    end

    def method_missing(method, *args, &block)
      if type == :files
        files[method]
      else
        *attribute_name, attribute_type = method.to_s.split('_')
        raise ArgumentError, "unknown attribute type '#{attribute_type}'" unless SUPPORTED_ATTRIBUTES.include?(attribute_type)
        self.send(attribute_type.pluralize)[attribute_name.join('_')]
      end
    end

    def dereference_column(id, options = {})
      column_id, reference_id = id.split(/\./).reverse
      column_options = options.dup
      column_options.merge!(id: column_id)

      if reference = references[reference_id]
        column_options.merge!(reference: reference)
      else
        raise ArgumentError, "dimension #{reference_id} not defined"
      end if reference_id

      Masamune::Schema::Column.new(column_options)
    end

    def extra(order = nil)
      return @extra unless order
      result = Set.new
      @extra.each do |file|
        filename = ::File.basename(file)
        if filename =~ /\A\d+_/
          number = filename.split('_').first.to_i
          result << file if number <= 0 && order == :pre
          result << file if number > 0 && order == :post
        else
          result << file if order == :pre
        end
      end
      result.to_a
    end
  end
end
