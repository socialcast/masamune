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

describe Masamune::Configuration do
  let(:environment) { Masamune::Environment.new }
  let(:instance) { described_class.new(environment: environment) }

  describe '.default_config_file' do
    subject { described_class.default_config_file }
    it { is_expected.to match(%r{config/masamune\.yml\.erb\Z}) }
  end

  describe '#default_config_file' do
    subject { instance.default_config_file }
    it { is_expected.to match(%r{config/masamune\.yml\.erb\Z}) }
  end

  describe '#params' do
    subject { instance.params }
    it { is_expected.to be_a(Hash) }
  end

  describe '#commands' do
    subject { instance.commands }
    it { is_expected.to be_a(Hashie::Mash) }
  end

  described_class.default_commands.each do |command_sym|
    describe "#commands.#{command_sym}" do
      subject { instance.commands.send(command_sym) }
      it { is_expected.to be_a(Hashie::Mash) }
    end
  end

  describe '#as_options' do
    subject { instance.as_options }
    it { is_expected.to eq([]) }

    context 'with dry_run: true and debug: true' do
      before do
        instance.debug = instance.dry_run = true
      end
      it { is_expected.to eq(['--debug', '--dry-run']) }
    end
  end

  describe '#load' do
    let(:yaml_file) do
      Tempfile.create('masamune').tap do |tmp|
        tmp.write(yaml)
        tmp.close
      end.path
    end

    subject(:result) { instance.load(yaml_file) }

    context 'with Hash params' do
      let(:yaml) do
        <<-YAML.strip_heredoc
        ---
          params:
            key_one: value_one
            key_two: value_two
        YAML
      end

      it do
        expect(result.params[:key_one]).to eq('value_one')
        expect(result.params[:key_two]).to eq('value_two')
      end
    end

    context 'with Array params' do
      let(:yaml) do
        <<-YAML.strip_heredoc
        ---
          params:
            - one
            - two
        YAML
      end

      it do
        expect { result }.to raise_error(ArgumentError, 'params section must only contain key value pairs')
      end
    end

    context 'with Hash paths' do
      let(:yaml) do
        <<-YAML.strip_heredoc
        ---
          paths:
            - foo_dir: ['/tmp/foo', {mkdir: true}]
            - bar_dir: '/tmp/bar'
        YAML
      end

      it do
        expect(result.filesystem.paths[:foo_dir]).to eq(['/tmp/foo', { mkdir: true }])
        expect(result.filesystem.paths[:bar_dir]).to eq(['/tmp/bar', {}])
      end
    end

    context 'with Hash commands' do
      let(:yaml) do
        <<-YAML.strip_heredoc
        ---
          commands:
            aws_emr:
              path: /opt/aws/bin/emr
              config_file: /etc/aws/emr_config
            hive:
              database: 'zombo'
        YAML
      end

      it do
        expect(result.commands.aws_emr.path).to eq('/opt/aws/bin/emr')
        expect(result.commands.aws_emr.config_file).to eq('/etc/aws/emr_config')
        expect(result.commands.hive.database).to eq('zombo')
      end
    end
  end
end
