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
  class StepFixture
    INDENT = 2

    def initialize(options = {})
      @file   = options[:file]
      @data   = options[:data]
      @type   = options[:type] || 'step'

      @data['input']  ||= []
      @data['output'] ||= []
    end

    class << self
      def load(options = {}, context = binding)
        file = options[:file]
        raise ArgumentError, "Fixture '#{file}' does not exist" unless File.exist?(file)
        YAML.safe_load(ERB.new(File.read(file)).result(context)).tap do |data|
          return new(options.merge(data: data))
        end
      end

      protected

      def suffix(options = {})
        "#{options[:type] || 'step'}_fixture.yml"
      end
    end

    def file_name
      @file
    end

    def path
      @path || File.dirname(@file)
    end

    def save
      FileUtils.mkdir_p(path)
      File.open(file_name, 'w') do |file|
        file.puts '---'
        file.puts 'input: |'
        @data['input'].split("\n").each do |line|
          file.puts line.indent(INDENT)
        end
        file.puts
        file.puts 'output: |'
        @data['output'].split("\n").each do |line|
          file.puts line.indent(INDENT)
        end
      end
      file_name
    end

    def input
      @data['input']
    end

    def output
      @data['output']
    end

    def output=(output)
      @data['output'] = output
    end
  end
end
