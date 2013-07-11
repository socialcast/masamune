require 'spec_helper'

describe Masamune::Commands::Streaming do
  # TODO use mock filesystem when checking for existance of files
  let(:extra_args) { ['-D', %q(map.output.key.field.separator='\t')] }
  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:input_option) { 'input.txt' }

  let(:general_options) do
    {
      input: input_option,
      output: 'output_dir',
      mapper: 'mapper.rb',
      reducer: 'reducer.rb',
      extra_args: extra_args
    }
  end
  let(:command_options) { [] }
  let(:context_options) { {} }

  before do
    Masamune.configure do |config|
      config.filesystem = filesystem
      config.hadoop_streaming[:options] = command_options
    end
  end

  subject(:instance) { described_class.new(general_options.merge(context_options)) }

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
        Masamune.logger.should_receive(:debug).with(/\ARemoving missing input/)
        instance.before_execute
      end
      its(:input) { should be_empty }
    end
  end

  describe '#command_args' do
    let(:pre_command_args) { ['hadoop', 'jar', Masamune.configuration.default_hadoop_streaming_jar] }
    let(:post_command_args) { ['-input', 'input.txt', '-mapper', 'mapper.rb', '-file', 'mapper.rb', '-reducer', 'reducer.rb', '-file', 'reducer.rb', '-output', 'output_dir'] }

    subject { instance.command_args }

    it { should == pre_command_args + extra_args + post_command_args }

    context 'with command_options' do
      let(:command_options) { [{'-cacheFile' => 'cache.rb'}] }

      it { should == pre_command_args + extra_args + command_options.map(&:to_a).flatten + post_command_args }
    end

    context 'with quote' do
      let(:context_options) { {quote: true} }
      let(:quoted_extra_args) { ['-D', %q(map.output.key.field.separator='"'\\\\t'"')] }

      subject { instance.command_args }

      it { should == pre_command_args + quoted_extra_args + post_command_args }
    end
  end
end
