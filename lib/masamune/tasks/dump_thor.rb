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

require 'masamune'
require 'thor'

module Masamune::Tasks
  class DumpThor < Thor
    include Masamune::Thor
    include Masamune::Transform::DefineSchema

    # FIXME need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :dump
    skip_lock!

    desc 'dump', 'Dump schema'
    method_option :type, :enum => ['psql', 'hql'], :desc => 'Schema type', :default => 'psql'
    method_option :with_index, :type => :boolean, :desc => 'Dump schema with indexes', :default => true
    method_option :with_foreign_key, :type => :boolean, :desc => 'Dump schema with foreign key constraints', :default => true
    method_option :with_unique_constraint, :type => :boolean, :desc => 'Dump schema with uniqueness constraints', :default => true
    def dump_exec
      print_catalog
      exit
    end
    default_task :dump_exec

    private

    def print_catalog
      case options[:type]
      when 'psql'
        puts define_schema(catalog, :postgres, options.slice(:with_index, :with_foreign_key, :with_unique_constraint).to_h.symbolize_keys)
      when 'hql'
        puts define_schema(catalog, :hive)
      end
    end
  end
end
