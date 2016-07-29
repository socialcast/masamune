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

describe Masamune::Commands::HadoopFilesystem do
  let(:configuration) { { options: options } }
  let(:options) { [] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    allow(delegate).to receive_message_chain(:configuration, :commands, :hadoop_filesystem).and_return(configuration)
  end

  describe '#command_args' do
    let(:attrs) { { extra: ['-ls', '/'] } }

    subject { instance.command_args }

    it { is_expected.to eq(['hadoop', 'fs', '-ls', '/']) }

    context 'with options' do
      let(:options) { [{ '--conf' => 'hadoop.conf' }] }

      it { is_expected.to eq(['hadoop', 'fs', '--conf', 'hadoop.conf', '-ls', '/']) }
    end
  end
end
