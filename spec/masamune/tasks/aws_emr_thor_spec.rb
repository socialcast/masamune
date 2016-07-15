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

require 'masamune/tasks/aws_emr_thor'

describe Masamune::Tasks::AwsEmrThor do
  context 'with help command ' do
    let(:command) { 'help' }
    it_behaves_like 'general usage'
  end

  Masamune::Tasks::AwsEmrThor::REQUIRE_CLUSTER_ID_ACTIONS.each do |action, _|
    context "with #{action} command" do
      let(:command) { action.underscore }

      context 'with --help' do
        let(:options) { ['--help'] }
        it_behaves_like 'command usage'
      end

      context 'without --cluster-id' do
        it_behaves_like 'raises Thor::RequiredArgumentMissingError', /No value provided for required options '--cluster-id'/
      end

      context 'with --cluster-id' do
        let(:options) { ['--cluster-id=j-XYZ'] }
        it do
          expect_any_instance_of(described_class).to receive(:aws_emr).with(hash_including(action: action, cluster_id: 'j-XYZ')).once.and_return(mock_success)
          execute_command
        end
      end
    end
  end

  Masamune::Tasks::AwsEmrThor::NO_REQUIRE_CLUSTER_ID_ACTIONS.each do |action, _|
    context "with #{action} command" do
      let(:command) { action.underscore }

      it do
        expect_any_instance_of(described_class).to receive(:aws_emr).with(hash_including(action: action)).once.and_return(mock_success)
        execute_command
      end

      context 'with --help' do
        let(:options) { ['--help'] }
        it_behaves_like 'command usage'
      end
    end
  end
end
