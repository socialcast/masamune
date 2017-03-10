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
  class PostgresThor < Thor
    include Masamune::Thor
    include Masamune::Actions::Postgres

    # FIXME: need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :postgres
    skip_lock!

    desc 'psql', 'Launch a Postgres session'
    method_option :file, aliases: '-f', desc: 'SQL from files'
    method_option :exec, aliases: '-e', desc: 'SQL from command line'
    method_option :output, aliases: '-o', desc: 'Save SQL output to file'
    method_option :csv, type: :boolean, desc: 'Report SQL output in CSV format', default: false
    method_option :retry, type: :boolean, desc: 'Retry SQL query in event of failure', default: false
    def psql_exec
      postgres_options = options.dup.with_indifferent_access
      postgres_options[:print] = true
      postgres_options[:max_retries] = 0 unless options[:retry]
      postgres(postgres_options)
    end
    default_task :psql_exec
  end
end
