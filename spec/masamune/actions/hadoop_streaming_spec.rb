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

require 'spec_helper'

describe Masamune::Actions::HadoopStreaming do
  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::Actions::HadoopStreaming
    end
  end

  let(:extra) { [] }
  let(:instance) { klass.new }

  before do
    instance.environment = Masamune::ExampleGroup
  end

  describe '.hadoop_streaming' do
    before do
      mock_command(/\Ahadoop/, mock_success)
    end

    subject { instance.hadoop_streaming(extra: extra) }

    it { is_expected.to be_success }

    context 'with jobflow' do
      before do
        allow(instance).to receive_message_chain(:configuration, :elastic_mapreduce).and_return({jobflow: 'j-XYZ'})
        mock_command(/\Ahadoop/, mock_failure)
        mock_command(/\Aelastic-mapreduce/, mock_success, StringIO.new('ssh fakehost exit'))
        mock_command(/\Assh fakehost hadoop/, mock_success)
      end

      it { is_expected.to be_success }
    end

    context 'with jobflow and extra' do
      let(:extra) { ['-D', 'EXTRA'] }

      before do
        allow(instance).to receive_message_chain(:configuration, :elastic_mapreduce).and_return({jobflow: 'j-XYZ'})
        mock_command(/\Ahadoop/, mock_failure)
        mock_command(/\Aelastic-mapreduce/, mock_success, StringIO.new('ssh fakehost exit'))
        mock_command(/\Assh fakehost -D EXTRA hadoop/, mock_failure)
        mock_command(/\Assh fakehost hadoop .*? -D EXTRA/, mock_success)
      end

      it { is_expected.to be_success }
    end
  end
end
