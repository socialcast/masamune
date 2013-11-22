require 'spec_helper'

describe Masamune::Commands::Streaming do
  let(:filesystem) { Masamune::MockFilesystem.new }

  let(:configuration) { {options: options, input: input_option, output: 'output_dir', mapper: 'mapper.rb', reducer: 'reducer.rb', extra_args: extra_args} }
  let(:options) { [] }
  let(:input_option) { 'input.txt' }
  let(:extra_args) { ['-D', %q(map.output.key.field.separator='\t')] }
  let(:attrs) { {} }

  subject(:instance) { described_class.new(configuration.merge(attrs)) }

  before do
    instance.stub(:filesystem) { filesystem }
  end

  describe '#before_execute' do
    context 'input path with suffix exists' do
      let(:input_option) { 'dir/input.txt' }
      before do
        filesystem.touch!('dir/input.txt')
        instance.before_execute
      end
      its(:input) { should == ['dir/input.txt'] }
    end

    context 'input path hadoop part' do
      let(:input_option) { 'dir/part_0000' }
      before do
        filesystem.touch!('dir/part_0000')
        instance.before_execute
      end
      its(:input) { should == ['dir/part_0000'] }
    end

    context 'input path directory' do
      let(:input_option) { 'dir' }
      before do
        filesystem.touch!('dir')
        instance.before_execute
      end
      its(:input) { should == ['dir/*'] }
    end

    context 'input path does not exist' do
      before do
        instance.logger.should_receive(:debug).with(/\ARemoving missing input/)
        instance.before_execute
      end
      its(:input) { should be_empty }
    end
  end

  describe '#command_args' do
    let(:pre_command_args) { ['hadoop', 'jar', described_class.default_hadoop_streaming_jar] }
    let(:post_command_args) { ['-input', 'input.txt', '-mapper', 'mapper.rb', '-file', 'mapper.rb', '-reducer', 'reducer.rb', '-file', 'reducer.rb', '-output', 'output_dir'] }

    subject { instance.command_args }

    it { should == pre_command_args + extra_args + post_command_args }

    context 'with options' do
      let(:options) { [{'-cacheFile' => 'cache.rb'}] }

      it { should == pre_command_args + extra_args + options.map(&:to_a).flatten + post_command_args }
    end

    context 'with quote' do
      let(:attrs) { {quote: true} }
      let(:quoted_extra_args) { ['-D', %q(map.output.key.field.separator='"'\\\\t'"')] }

      subject { instance.command_args }

      it { should == pre_command_args + quoted_extra_args + post_command_args }
    end
  end
end
