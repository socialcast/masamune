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

require 'delegate'

module Masamune::Commands
  class HadoopFilesystem < SimpleDelegator
    DEFAULT_ATTRIBUTES =
    {
      path: 'hadoop',
      options: [],
      extra: [],
      block: nil,
      print: false
    }.with_indifferent_access.freeze

    def initialize(delegate, attrs = {})
      super delegate
      DEFAULT_ATTRIBUTES.merge(configuration.commands.hadoop_filesystem).merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    def command_args
      args = []
      args << @path
      args << 'fs'
      args << @options.map(&:to_a)
      args << @extra
      args.flatten
    end

    def handle_stdout(line, _line_no)
      @block.call(line) if @block
      console(line) if @print
    end
  end
end
