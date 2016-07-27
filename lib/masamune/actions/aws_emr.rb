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

module Masamune::Actions
  module AwsEmr
    extend ActiveSupport::Concern

    def aws_emr(opts = {})
      opts = opts.to_hash.symbolize_keys

      command = Masamune::Commands::AwsEmr.new(environment, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, configuration.commands.aws_emr.slice(:retries, :backoff).merge(opts))
      command = Masamune::Commands::Shell.new(command, opts)

      command.interactive? ? command.replace : command.execute
    end

    def validate_cluster_id!
      cluster_id = configuration.commands.aws_emr[:cluster_id]
      raise ::Thor::RequiredArgumentMissingError, "No value provided for required options '--cluster-id'" unless cluster_id
      raise ::Thor::RequiredArgumentMissingError, %(AWS EMR cluster '#{cluster_id}' does not exist) unless aws_emr(action: 'describe-cluster', cluster_id: cluster_id, fail_fast: false).success?
    end

    included do |base|
      base.class_option :cluster_id, desc: 'AWS EMR cluster_id ID (Hint: `masamune-emr-aws list-clusters`)' if defined?(base.class_option)
      base.after_initialize(:early) do |thor, options|
        next unless thor.configuration.commands.aws_emr.any?
        next unless thor.configuration.commands.aws_emr.fetch(:enabled, true)
        thor.configuration.commands.aws_emr[:cluster_id] = options[:cluster_id] if options[:cluster_id]
        next unless options[:initialize]
        thor.validate_cluster_id!
      end if defined?(base.after_initialize)
    end
  end
end
