require 'spec_helper'

describe Masamune::Commands::Streaming do
  let(:extra_args) { ['-D', %q(map.output.key.field.separator='\t')] }

  let(:default_options) do
    {
      input: 'input',
      output: 'output',
      mapper: 'mapper.rb',
      reducer: 'reduerc.rb',
      extra_args: extra_args
    }
  end
  let(:command_options) { [] }
  let(:special_options) { {} }

  before do
    Masamune.configuration.add_command_options(:streaming) do
      command_options
    end
  end
  let(:instance) { Masamune::Commands::Streaming.new(default_options.merge(special_options)) }

  describe '#command_args' do
    let(:pre_command_args) { ['hadoop', 'jar', Masamune::configuration.hadoop_streaming_jar] }
    let(:post_command_args) { ['-input', 'input', '-mapper', 'mapper.rb', '-file', 'mapper.rb', '-reducer', 'reduerc.rb', '-file', 'reduerc.rb', '-output', 'output'] }

    subject { instance.command_args }

    it { should == pre_command_args + extra_args + post_command_args }

    context 'with command_options' do
      let(:command_options) { ['-cacheFile', 'cache.rb' ] }

      it { should == pre_command_args + extra_args + command_options  + post_command_args }
    end

    context 'with quote' do
      let(:special_options) { {quote: true} }
      let(:quoted_extra_args) { ['-D', %q(map.output.key.field.separator='"'\\\\t'"')] }

      subject { instance.command_args }

      it { should == pre_command_args + quoted_extra_args + post_command_args }
    end
  end
end
