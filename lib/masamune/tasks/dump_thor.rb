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

require 'masamune'
require 'thor'

module Masamune::Tasks
  class DumpThor < Thor
    include Masamune::Thor
    include Masamune::Actions::DateParse
    include Masamune::Transform::DefineSchema

    # FIXME need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :dump
    skip_lock!

    desc 'dump', 'Dump schema'
    method_option :type, :enum => ['psql', 'hql'], :desc => 'Schema type', :default => 'psql'
    method_option :section, :enum => ['pre', 'post', 'all'], :desc => 'Schema section', :default => 'all'
    method_option :exclude, :type => :array, :desc => 'Exclude tables matching globs', :default => []
    method_option :skip_indexes, :type => :boolean, :desc => 'Disable indexes', :default => false
    def dump_exec
      print_catalog
      exit
    end
    default_task :dump_exec

    private

    def print_catalog
      case options[:type]
      when 'psql'
        puts define_schema(catalog, :postgres, define_schema_options)
      when 'hql'
        puts define_schema(catalog, :hive, define_schema_options)
      end
    end

    def define_schema_options
      {
        exclude: options[:exclude],
        section: options[:section].to_sym,
        skip_indexes: options[:skip_indexes],
        start_date: start_date,
        stop_date: stop_date
      }.reject { |_, v| v.blank? }
    end
  end
end
