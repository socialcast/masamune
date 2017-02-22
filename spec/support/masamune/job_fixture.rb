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

require 'active_support/core_ext/string/indent'

module Masamune
  class JobFixture
    INDENT = 2

    attr_accessor :name

    def initialize(options = {})
      @path     = options[:path]
      @name     = options[:name] || options[:fixture]
      @file     = options[:file]
      @data     = options[:data]
      @type     = options[:type]
      @context  = options[:context]

      @data['inputs']  ||= []
      @data['outputs'] ||= []
    end

    class << self
      def load(options = {}, context = binding)
        file = file_name(options)
        raise ArgumentError, "Fixture '#{file}' does not exist" unless File.exist?(file)
        YAML.safe_load(ERB.new(File.read(file)).result(context)).tap do |data|
          return new(options.merge(data: data, context: context))
        end
      end

      def file_name(options = {})
        return options[:file] if options[:file]
        File.join(options[:path], [options[:name] || options[:fixture], suffix(options)].compact.join('.'))
      end

      protected

      def suffix(options = {})
        "#{options[:type] || 'job'}_fixture.yml"
      end
    end

    def file_name
      self.class.file_name(file: @file, path: @path, name: @name, type: @type)
    end

    def path
      @path || File.dirname(@file)
    end

    def save
      FileUtils.mkdir_p(path)
      File.open(file_name, 'w') do |file|
        file.puts '---'
        file.puts 'inputs:'
        @data['inputs'].each do |input|
          file.puts '-'.indent(INDENT)
          serialize(input) do |elem|
            file.puts elem.indent(INDENT * 2)
          end
        end
        file.puts
        file.puts 'outputs:'
        @data['outputs'].each do |output|
          file.puts '-'.indent(INDENT)
          serialize(output) do |elem|
            file.puts elem.indent(INDENT * 2)
          end
        end
      end
      file_name
    end

    def inputs
      @inputs ||= begin
        @data['inputs'].map do |input|
          if input['reference']
            raise ArgumentError, "reference in #{file_name} requires fixture" unless input['reference']['fixture'] || input['reference']['file']
            reference = self.class.load({ path: input['reference']['path'] || path, name: input['reference']['fixture'], file: input['reference']['file'], type: @type }, @context)
            section = input['reference']['section'] || 'outputs'
            reference.send(section) if reference.respond_to?(section)
          else
            input
          end
        end.flatten.compact
      end
    end

    def outputs
      @data['outputs']
    end

    private

    def serialize(hash, level = 0)
      hash.each do |key, value|
        case value
        when String
          if value.split("\n").count > 1
            yield "#{key}: |".indent(level * INDENT)
            value.split("\n").each do |line|
              yield line.strip.indent((level + 1) * INDENT)
            end
          else
            yield "#{key}: #{value}".indent(level * INDENT)
          end
        when Hash
          yield "#{key}:".indent(level * INDENT)
          serialize(value, level + 1) do |next_hash|
            yield next_hash
          end
        end
      end
    end
  end
end
