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

describe Masamune::Commands::HadoopStreaming do
  let(:filesystem) { Masamune::MockFilesystem.new }

  let(:configuration) { { options: options, input: input_option, output: 'output_dir', mapper: 'mapper.rb', reducer: 'reducer.rb', extra: extra } }
  let(:options) { [] }
  let(:input_option) { 'input.txt' }
  let(:extra) { ['-D', 'map.output.key.field.separator=\'\t\''] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    allow(delegate).to receive(:filesystem).and_return(filesystem)
    allow(delegate).to receive(:console).and_return(double)
    allow(delegate).to receive(:logger).and_return(double)
    allow(delegate).to receive_message_chain(:configuration, :commands, :hadoop_streaming).and_return(configuration)
  end

  describe '#before_execute' do
    subject(:input) { instance.input }

    context 'input path with suffix exists' do
      let(:input_option) { 'dir/input.txt' }
      before do
        filesystem.touch!('dir/input.txt')
        instance.before_execute
      end
      it { is_expected.to eq(['dir/input.txt']) }
    end

    context 'input path hadoop part' do
      let(:input_option) { 'dir/part_0000' }
      before do
        filesystem.touch!('dir/part_0000')
        instance.before_execute
      end
      it { is_expected.to eq(['dir/part_0000']) }
    end

    context 'input path directory' do
      let(:input_option) { 'dir' }
      before do
        filesystem.touch!('dir')
        instance.before_execute
      end
      it { is_expected.to eq(['dir/*']) }
    end

    context 'input path does not exist' do
      before do
        expect(instance.logger).to receive(:debug).with(/\ARemoving missing input/)
        instance.before_execute
      end
      it { is_expected.to be_empty }
    end
  end

  describe '#command_args' do
    let(:pre_command_args) { ['hadoop', 'jar', described_class.default_hadoop_streaming_jar] }
    let(:post_command_args) { ['-input', 'input.txt', '-mapper', 'mapper.rb', '-file', 'mapper.rb', '-reducer', 'reducer.rb', '-file', 'reducer.rb', '-output', 'output_dir'] }

    subject { instance.command_args }

    it { is_expected.to eq(pre_command_args + extra + post_command_args) }

    context 'with options' do
      let(:options) { [{ '-cacheFile' => 'cache.rb' }] }

      it { is_expected.to eq(pre_command_args + extra + options.map(&:to_a).flatten + post_command_args) }
    end

    context 'with quote' do
      let(:attrs) { { quote: true } }
      let(:quoted_extra) { ['-D', %q(map.output.key.field.separator='"'\\\\t'"')] }

      subject { instance.command_args }

      it { is_expected.to eq(pre_command_args + quoted_extra + post_command_args) }
    end
  end
end
