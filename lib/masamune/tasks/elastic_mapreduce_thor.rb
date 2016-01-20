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
  class ElasticMapreduceThor < Thor
    include Masamune::Thor
    include Masamune::Actions::ElasticMapreduce

    # FIXME need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :elastic_mapreduce
    skip_lock!

    desc 'elastic_mapreduce', 'Launch an ElasticMapReduce ssh session'
    class_option :template, :type => :string, :aliases => '-t', :desc => 'Execute named template command'
    class_option :params, :type => :hash, :aliases => '-p', :desc => 'Bind params to named template command', :default => {}
    def elastic_mapreduce_exec
      elastic_mapreduce_options = options.dup.with_indifferent_access
      elastic_mapreduce_options.merge!(interactive: true)
      elastic_mapreduce_options.merge!(extra: extra_or_ssh)
      elastic_mapreduce(elastic_mapreduce_options)
    end
    default_task :elastic_mapreduce_exec

    no_tasks do
      after_initialize(:first) do |thor, options|
        begin
          thor.extra += thor.configuration.bind_template(:elastic_mapreduce, options[:template], options[:params]) if options[:template]
        rescue ArgumentError => e
          raise ::Thor::MalformattedArgumentError, e.to_s
        end
      end

      def extra_or_ssh
        self.extra.any? ? self.extra : ['--ssh']
      end
    end
  end
end
