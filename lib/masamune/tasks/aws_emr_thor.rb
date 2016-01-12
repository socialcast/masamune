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
  class AwsEmrThor < Thor
    include Masamune::Thor
    include Masamune::Actions::AwsEmr

    # FIXME need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :aws_emr
    skip_lock!

    REQUIRE_CLUSTER_ID_ACTIONS = 
    {
      'describe-cluster'    => 'Describe an AWS EMR cluster',
      'list-instances'      => 'List instances for an AWS EMR cluster',
      'add-instance-groups' => 'Add instances to an AWS EMR cluster',
      'ssh'                 => 'Launch an AWS EMR ssh session',
      'wait'                => 'Wait for an AWS EMR cluster to start'
    }

    REQUIRE_CLUSTER_ID_ACTIONS.each do |action, description|
      desc action, description
      method_option :cluster_id, :desc => "AWS EMR cluster_id ID (Hint: `masamune-emr-aws list-clusters`)"
      define_method(action.underscore) do
        raise Thor::RequiredArgumentMissingError, "No value provided for required options '--cluster-id'" unless options[:cluster_id]
        aws_emr(aws_emr_options(action))
      end
    end

    NO_REQUIRE_CLUSTER_ID_ACTIONS = 
    {
      'create-cluster'          => 'Create an AWS EMR cluster',
      'list-clusters'           => 'List existing AWS EMR clusters',
      'modify-instance-groups'  => 'Modify instance groups for an AWS EMR cluster',
      'terminate-clusters'      => 'Terminate one or more AWS EMR clusters'
    }

    NO_REQUIRE_CLUSTER_ID_ACTIONS.each do |action, description|
      desc action, description
      define_method(action.underscore) do
        aws_emr(aws_emr_options(action))
      end
    end

    no_tasks do
      def aws_emr_options(action)
        options.dup.with_indifferent_access.tap do |opts|
          opts.merge!(interactive: true)
          opts.merge!(action: action)
          opts.merge!(extra: self.extra)
        end
      end
    end
  end
end
