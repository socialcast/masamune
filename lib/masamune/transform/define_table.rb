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

module Masamune::Transform
  module DefineTable
    extend ActiveSupport::Concern

    def define_table(target, options = {})
      return if target.implicit
      return if exclude_table?(target, options)
      child_tables = target.children.map { |child| define_table(child, options.except(:files)) }
      Operator.new(*child_tables, __method__, target: target, **options).tap do |operator|
        logger.debug("#{target.id}\n" + operator.to_s) if target.debug
      end
    end

    private

    def exclude_table?(table, options = {})
      exclude_matchers(options[:exclude]).any? { |matcher| matcher =~ table.name }
    end

    def exclude_matchers(exclude)
      Array.wrap(exclude).map do |input|
        case input
        when String
          glob_to_regexp(input)
        when Regexp
          input
        end
      end
    end

    def glob_to_regexp(input)
      if input.include?('*')
        %r|\A#{Regexp.escape(input).gsub('\\*', '.*?')}|
      else
        /\A#{Regexp.escape(input)}\z/
      end
    end
  end
end
