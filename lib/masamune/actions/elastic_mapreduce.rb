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

module Masamune::Actions
  module ElasticMapreduce
    extend ActiveSupport::Concern

    def elastic_mapreduce(opts = {})
      opts = opts.to_hash.symbolize_keys
      opts[:jobflow] = resolve_jobflow(opts[:jobflow]) if opts[:jobflow]

      command = Masamune::Commands::Interactive.new(environment, :interactive => opts.fetch(:interactive, false))
      command = Masamune::Commands::ElasticMapReduce.new(command, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, configuration.elastic_mapreduce.slice(:retries, :backoff).merge(opts))
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    def defined_jobflows
      @defined_jobflows ||= configuration.elastic_mapreduce.fetch(:jobflows, {}).symbolize_keys
    end

    def resolve_jobflow(jobflow)
      return unless jobflow
      defined_jobflows[jobflow.to_sym] || jobflow
    end

    def jobflow_required?
      extra.empty?
    end

    def validate_jobflow!
      return unless jobflow_required?
      jobflow = configuration.elastic_mapreduce[:jobflow]
      raise ::Thor::RequiredArgumentMissingError, "No value provided for required options '--jobflow'" unless jobflow
      raise ::Thor::RequiredArgumentMissingError, %Q(Value '#{jobflow}' for '--jobflow' doesn't exist) unless elastic_mapreduce(extra: '--list', jobflow: jobflow, fail_fast: false).success?
    end

    included do |base|
      base.class_option :jobflow, :aliases => '-j', :desc => 'Elastic MapReduce jobflow ID (Hint: elastic-mapreduce --list)' if defined?(base.class_option)
      base.after_initialize(:early) do |thor, options|
        next unless thor.configuration.elastic_mapreduce.any?
        next unless thor.configuration.elastic_mapreduce.fetch(:enabled, true)
        thor.configuration.elastic_mapreduce[:jobflow] = thor.resolve_jobflow(options[:jobflow] || thor.configuration.elastic_mapreduce[:jobflow])
        next unless options[:initialize]
        thor.validate_jobflow!
      end if defined?(base.after_initialize)
    end
  end
end
