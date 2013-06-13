require 'spec_helper'

describe Masamune::Commands::Streaming do
  # TODO use mock filesystem when checking for existance of files
  let(:extra_args) { ['-D', %q(map.output.key.field.separator='\t')] }
  let(:filesystem) { MockFilesystem.new }

  let(:general_options) do
    {
      input: 'input.txt',
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
      config.add_command_options(:streaming) do
        command_options
      end
    end
  end

  subject(:instance) { described_class.new(general_options.merge(context_options)) }

  describe '#before_execute' do
    context 'input path exists' do
      before do
        filesystem.touch!('input.txt')
        instance.before_execute
      end
      its(:input) { should == ['input.txt'] }
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
    let(:pre_command_args) { ['hadoop', 'jar', Masamune.configuration.hadoop_streaming_jar] }
    let(:post_command_args) { ['-input', 'input.txt', '-mapper', 'mapper.rb', '-file', 'mapper.rb', '-reducer', 'reducer.rb', '-file', 'reducer.rb', '-output', 'output_dir'] }

    subject { instance.command_args }

    it { should == pre_command_args + extra_args + post_command_args }

    context 'with command_options' do
      let(:command_options) { ['-cacheFile', 'cache.rb' ] }

      it { should == pre_command_args + extra_args + command_options  + post_command_args }
    end

    context 'with quote' do
      let(:context_options) { {quote: true} }
      let(:quoted_extra_args) { ['-D', %q(map.output.key.field.separator='"'\\\\t'"')] }

      subject { instance.command_args }

      it { should == pre_command_args + quoted_extra_args + post_command_args }
    end
  end
end
