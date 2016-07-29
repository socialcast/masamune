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

describe Masamune::Commands::Postgres do
  let(:configuration) { { path: 'psql', database: 'postgres', options: options } }
  let(:options) { [] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    allow(delegate).to receive(:logger).and_return(double)
    allow(delegate).to receive(:console).and_return(double)
    allow(delegate).to receive_message_chain(:configuration, :commands, :postgres).and_return(configuration)
  end

  describe '#stdin' do
    context 'with input' do
      let(:attrs) { { input: 'SELECT * FROM table;' } }
      subject(:stdin) { instance.stdin }

      it { is_expected.to be_a(StringIO) }

      describe '#string' do
        subject { stdin.string }
        it { is_expected.to eq('SELECT * FROM table;') }
      end
    end
  end

  describe '#command_args' do
    let(:default_command) { ['psql', '--host=localhost', '--dbname=postgres', '--username=postgres', '--no-password', '--set=ON_ERROR_STOP=1'] }

    subject do
      instance.before_execute
      instance.command_args
    end

    it { is_expected.to eq(default_command) }

    context 'with options' do
      let(:options) { [{ '-A' => nil }] }
      it { is_expected.to eq([*default_command, '-A']) }
    end

    context 'with exec' do
      let(:attrs) { { exec: 'SELECT * FROM table;' } }
      before do
        expect(instance).to receive(:exec_file).and_return('zomg.psql')
      end
      it { is_expected.to eq([*default_command, '--file=zomg.psql']) }
    end

    context 'with file' do
      let(:attrs) { { file: 'zomg.psql' } }
      it { is_expected.to eq([*default_command, '--file=zomg.psql']) }
    end

    context 'with file and debug' do
      let(:attrs) { { file: 'zomg.psql', debug: true } }
      before do
        expect(File).to receive(:read).with('zomg.psql').and_return('SHOW TABLES;')
        expect(instance.logger).to receive(:debug).with("zomg.psql:\nSHOW TABLES;")
      end
      it { is_expected.to eq([*default_command, '--file=zomg.psql']) }
    end

    context 'with file and exec' do
      let(:attrs) { { file: 'zomg.psql', exec: 'SELECT * FROM table;' } }
      it { expect { subject }.to raise_error(/Cannot specify both file and exec/) }
    end

    context 'with template file' do
      let(:attrs) { { file: 'zomg.psql.erb' } }
      before do
        expect(Masamune::Template).to receive(:render_to_file).with('zomg.psql.erb', {}).and_return('zomg.psql')
      end
      it { is_expected.to eq([*default_command, '--file=zomg.psql']) }
    end

    context 'with template file and debug' do
      let(:attrs) { { file: 'zomg.psql.erb', debug: true } }
      before do
        expect(Masamune::Template).to receive(:render_to_file).with('zomg.psql.erb', {}).and_return('zomg.psql')
        expect(File).to receive(:read).with('zomg.psql').and_return('SHOW TABLES;')
        expect(instance.logger).to receive(:debug).with("zomg.psql:\nSHOW TABLES;")
      end
      it { is_expected.to eq([*default_command, '--file=zomg.psql']) }
    end

    context 'with variables and no file' do
      let(:attrs) { { variables: { R: 'R2D2', C: 'C3PO' } } }
      it { is_expected.to eq(default_command) }
    end

    context 'with variables and file' do
      let(:attrs) { { file: 'zomg.psql', variables: { R: 'R2D2', C: 'C3PO' } } }
      it { is_expected.to eq([*default_command, '--file=zomg.psql', "--set=R='R2D2'", "--set=C='C3PO'"]) }
    end

    context 'with csv' do
      let(:attrs) { { csv: true } }
      it { is_expected.to eq([*default_command, '--no-align', '--field-separator=,', '--pset=footer']) }
    end

    context 'with tuple_output' do
      let(:attrs) { { tuple_output: true } }
      it { is_expected.to eq([*default_command, '--pset=tuples_only']) }
    end
  end

  describe '#failure_message' do
    let(:status_code) { 1 }

    context 'when error detected' do
      before do
        expect(instance.logger).to receive(:debug).at_least(3).times
        instance.handle_stderr('Everything is OK', 0)
        instance.handle_stderr('psql:/var/tmp/schema.psql ERROR: Something went wrong', 1)
        instance.handle_stderr('Wha happen', 2)
      end

      subject { instance.failure_message(status_code) }

      it { is_expected.to eq('Something went wrong') }
    end

    context 'when no error detected' do
      subject { instance.failure_message(status_code) }

      it { is_expected.to eq('psql failed without error') }
    end
  end

  it_should_behave_like Masamune::Commands::PostgresCommon
end
