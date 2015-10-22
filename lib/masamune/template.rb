#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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

require 'tilt'
require 'pathname'

module Masamune
  class Template
    def initialize(paths = [])
      @paths = Array.wrap(paths)
      @paths << File.join(File.dirname(__FILE__), 'transform')
    end

    def render(template, parameters = {})
      resolved_template = resolve_file(template)
      self.class.combine Tilt.new(resolved_template, nil, trim: '->').render(self, parameters)
    end

    private

    # TODO unify with resolve_path
    def resolve_file(partial_file)
      return partial_file if Pathname.new(partial_file).absolute?
      @paths.each do |path|
        file = File.expand_path(File.join(path, partial_file))
        if File.exists?(file) && File.file?(file)
          @paths << File.dirname(file)
          return file 
        end
      end
      raise IOError, "File not found: #{partial_file}"
    end

    class << self
      def render_to_file(template, parameters = {})
        Tempfile.new('masamune').tap do |file|
          file.write(render_to_string(template, parameters))
          file.close
        end.path
      end

      def render_to_string(template, parameters = {})
        instance = self.new(File.dirname(template))
        combine instance.render(template, parameters)
      end

      def combine(*a)
        strip_newlines(strip_comments(a.join("\n"), /^--.*$/)) + "\n"
      end

      def strip_newlines(s)
        s.gsub(/^\n+/, "\n").lstrip.strip
      end

      def strip_comments(s, comment)
        strip_newlines s.gsub(comment, '')
      end
    end
  end
end
