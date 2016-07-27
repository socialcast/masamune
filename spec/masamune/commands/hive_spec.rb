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

describe Masamune::Commands::Hive do
  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:configuration) { { options: options } }
  let(:options) { [] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  let(:local_file) { File.join(Dir.tmpdir, SecureRandom.hex + '.txt') }
  let(:mock_tmpdir) { 'mock' }
  let(:remote_file) { filesystem.path(:tmp_dir, mock_tmpdir, File.basename(local_file)) }

  before do
    FileUtils.touch(local_file)
    filesystem.add_path(:tmp_dir, File.join(Dir.tmpdir, SecureRandom.hex))
    allow(filesystem).to receive(:mktempdir!) { filesystem.path(:tmp_dir, mock_tmpdir) }

    allow(delegate).to receive(:filesystem) { filesystem }
    allow(delegate).to receive(:logger).and_return(double)
    allow(delegate).to receive(:console).and_return(double)
    allow(delegate).to receive_message_chain(:configuration, :commands, :hive).and_return(configuration)
  end

  describe '#command_args' do
    let(:default_command) { ['hive', '--database', 'default'] }

    subject do
      instance.before_execute
      instance.command_args
    end

    it { is_expected.to eq(default_command) }

    context 'with command attrs' do
      let(:options) { [{ '-d' => 'DATABASE=development' }] }
      it { is_expected.to eq([*default_command, '-d', 'DATABASE=development']) }
    end

    context 'with file' do
      let(:attrs) { { file: local_file } }
      it { is_expected.to eq([*default_command, '-f', remote_file]) }
    end

    context 'with file and debug' do
      let(:attrs) { { file: local_file, debug: true } }
      before do
        expect(File).to receive(:read).with(local_file).and_return('SHOW TABLES;')
        expect(instance.logger).to receive(:debug).with("#{local_file}:\nSHOW TABLES;")
      end
      it { is_expected.to eq([*default_command, '-f', remote_file]) }
    end

    context 'with exec' do
      let(:attrs) { { exec: 'SELECT * FROM table;' } }
      before do
        expect(instance).to receive(:exec_file).and_return(remote_file)
      end
      it { is_expected.to eq([*default_command, '-f', remote_file]) }
    end

    context 'with file and exec' do
      let(:attrs) { { file: local_file, exec: 'SELECT * FROM table;' } }
      it { expect { instance }.to raise_error ArgumentError, 'Cannot specify both file and exec' }
    end

    context 'with variables' do
      let(:attrs) { { file: local_file, variables: { R: 'R2DO', C: 'C3PO' } } }
      it { is_expected.to eq([*default_command, '-f', remote_file, '-d', 'R=R2DO', '-d', 'C=C3PO']) }
    end

    context 'with setup files' do
      before do
        filesystem.touch!('setup_a.hql', 'setup_b.hql')
      end

      let(:attrs) { { setup_files: ['setup_a.hql', 'setup_b.hql'] } }
      it { is_expected.to eq([*default_command, '-i', 'setup_a.hql', '-i', 'setup_b.hql']) }
    end

    context 'with template file' do
      let(:attrs) { { file: 'zomg.hql.erb' } }
      before do
        expect(Masamune::Template).to receive(:render_to_file).with('zomg.hql.erb', {}).and_return('zomg.hql')
        expect_any_instance_of(Masamune::MockFilesystem).to receive(:copy_file_to_file)
      end
      it { is_expected.to eq([*default_command, '-f', filesystem.get_path(:tmp_dir, mock_tmpdir, 'zomg.hql')]) }
    end

    context 'with template file and debug' do
      let(:attrs) { { file: 'zomg.hql.erb', debug: true } }
      before do
        expect(Masamune::Template).to receive(:render_to_file).with('zomg.hql.erb', {}).and_return('zomg.hql')
        expect(File).to receive(:read).with('zomg.hql').and_return('SHOW TABLES;')
        expect(instance.logger).to receive(:debug).with("zomg.hql:\nSHOW TABLES;")
        expect_any_instance_of(Masamune::MockFilesystem).to receive(:copy_file_to_file)
      end
      it { is_expected.to eq([*default_command, '-f', filesystem.get_path(:tmp_dir, mock_tmpdir, 'zomg.hql')]) }
    end
  end

  describe '#handle_stdout' do
    let(:buffer) { StringIO.new }
    let(:delimiter) { "\t" }
    let(:attrs) { { buffer: buffer, delimiter: delimiter, csv: true } }

    context 'encode NULL' do
      let(:input_row) { ['A', 'NULL', 'B', 'C', '', 'E'].join(delimiter) }
      let(:output_row) { ['A', nil, 'B', 'C', nil, 'E'].join(',') }

      before do
        instance.handle_stdout(input_row, 0)
      end

      it { expect(buffer.string).to eq(output_row + "\n") }
    end

    context 'non zero exit' do
      let(:input_row) { 'Query returned non-zero code: 1, cause: Problem compiling' }

      it { expect { instance.handle_stdout(input_row, 0) }.to raise_error(SystemExit, input_row) }
    end
  end
end
