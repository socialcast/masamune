#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'csv'

module Masamune::Schema
  class Map
    class JSONEncoder < SimpleDelegator
      def initialize(io, store)
        super io
        @store = store
      end

      def gets(*a)
        line = __getobj__.gets(*a)
        return unless line
        return line if skip?
        encode(line, separator).join(separator)
      end

      private

      def skip?
        @store.json_encoding == :quoted
      end

      def encode(line, separator)
        fields = []
        buffer = ''
        nested = false
        line.strip.each_char do |char|
          case char
          when '{'
            buffer << char
            nested = true
          when '}'
            buffer << char
            nested = false
          when separator
            if nested
              buffer << char
            else
              fields << quote(buffer)
              buffer = ''
            end
          else
            buffer << char
          end
        end
        fields << quote(buffer)
        fields.compact
      end

      def quote(buffer)
        return buffer if buffer =~ /\A".*"\z/
        %Q{"#{buffer.gsub('"', '""')}"}
      end

      def separator
        @separator ||=
        case @store.format
        when :tsv then "\t"
        when :csv then ','
        end
      end
    end

    class Buffer
      extend Forwardable

      def_delegators :@io, :flush, :path

      def initialize(map, table)
        @map      = map
        @table    = table
        @store    = table.store
        @line     = 0
      end

      def bind(io)
        @io = io.set_encoding('binary', 'UTF-8', undef: :replace)
        @csv = nil
      end

      def each(&block)
        raise 'must call Buffer#bind first' unless @io
        CSV.parse(JSONEncoder.new(@io, @store), options.merge(headers: @store.headers || @table.columns.keys)) do |data|
          next if data.to_s =~ /\A#/
          yield safe_row(data)
          @line += 1
        end
      end

      def append(data)
        raise 'must call Buffer#bind first' unless @io
        row = Masamune::Schema::Row.new(parent: @table, values: data.to_hash)
        write_headers = @store.headers && @line < 1
        @csv ||= CSV.new(@io, options.merge(headers: row.headers, write_headers: write_headers))
        if row.missing_required_columns.any?
          missing_required_column_names = row.missing_required_columns.map(&:name)
          @map.skip_or_raise(self, row, "missing required columns '#{missing_required_column_names.join(', ')}'")
        else
          @csv << row.serialize if append?(row.serialize)
        end
        @line += 1
      end

      def line
        @line
      end

      private

      def options
        {skip_blanks: true}.tap do | opts|
          opts[:col_sep] = "\t" if @store.format == :tsv
        end
      end

      def safe_row(data)
        row = Masamune::Schema::Row.new(parent: @table, values: data.to_hash, strict: false)
        row.to_hash
      rescue
        @map.skip_or_raise(self, data, 'failed to parse')
      end

      def append?(elem)
        return true unless @map.distinct
        @seen ||= Set.new
        @seen.add?(elem)
      end
    end

    DEFAULT_ATTRIBUTES =
    {
      source:    nil,
      target:    nil,
      columns:   nil,
      store:     nil,
      function:  ->(row, *_) { row },
      distinct:  false,
      fail_fast: false,
      debug:     false
    }

    DEFAULT_ATTRIBUTES.keys.each do |attr|
      attr_accessor attr
    end

    def initialize(opts = {})
      opts.symbolize_keys!
      raise ArgumentError, 'required parameter source: missing' unless opts.key?(:source)
      raise ArgumentError, 'required parameter target: missing' unless opts.key?(:target)
      DEFAULT_ATTRIBUTES.merge(opts).each do |name, value|
        public_send("#{name}=", value)
      end
      @skipped = 0
    end

    def source=(source)
      @source = source
    end

    # FIXME: avoid implict conversions
    def target=(target)
      @target = target.type == :four ? target.ledger_table : target
    end

    def intermediate_columns
      output = function.call(default_row(source.columns), 0)
      example = Array.wrap(output).first
      raise ArgumentError, "function for map between '#{source.name}' and '#{target.name}' does not return output for default input" unless example
      example.keys
    end

    def intermediate
      target.stage_table(columns: columns || intermediate_columns, inherit: false)
    end

    def apply(input_files, output_file)
      input_buffer  = Buffer.new(self, source)
      output_buffer = Buffer.new(self, intermediate)
      self.class.convert_files(input_files).each do |input_file|
        open_stream(input_file, 'r') do |input_stream|
          input_buffer.bind(input_stream)
          open_stream(output_file, 'a+') do |output_stream|
            output_buffer.bind(output_stream)
            apply_buffer(input_buffer, output_buffer)
          end
        end
      end
      intermediate
    end

    def open_stream(file, mode, &block)
      case file
      when IO, StringIO
        file.flush
        yield file
      when String, Tempfile
        File.open(file, mode) do |io|
          yield io
        end
      end
    end

    def skip_or_raise(buffer, row, message)
      message = 'failed to process' if message.nil? || message.blank?
      trace = { message: message, source: source.name, target: target.name, file: buffer.try(:path), line: buffer.try(:line), row: row.try(:to_hash) }
      if fail_fast
        @store.logger.error(message)
        @store.logger.debug(trace)
        raise message
      else
        @store.logger.warn(message)
        @store.logger.debug(trace)
      end
    end

    class << self
      def convert_file(file)
        if file.respond_to?(:path)
          file.flush if file.respond_to?(:flush) && file.respond_to?(:open?) && file.open?
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
    end

    private

    def default_row(columns)
      {}.with_indifferent_access.tap do |row|
        columns.each do |_, column|
          row[column.compact_name] = column.default_ruby_value
        end
      end
    end

    def apply_buffer(input_buffer, output_buffer)
      input_buffer.each do |input|
        safe_apply_function(input_buffer, input) do |output|
          output_buffer.append output
        end
      end
      output_buffer.flush
    end

    def safe_apply_function(input_buffer, input, &block)
      return unless input
      Array.wrap(function.call(input, input_buffer.line)).each do |output|
        yield output
      end
    rescue => e
      skip_or_raise(input_buffer, input, e.message)
    end
  end
end
