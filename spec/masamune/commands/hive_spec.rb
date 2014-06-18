require 'spec_helper'

describe Masamune::Commands::Hive do
  let(:filesystem) { Masamune::MockFilesystem.new }
  let(:configuration) { {:options => options} }
  let(:options) { [] }
  let(:attrs) { {} }

  let(:delegate) { double }
  let(:instance) { described_class.new(delegate, attrs) }

  before do
    filesystem.add_path(:tmp_dir, File.join(Dir.tmpdir, SecureRandom.hex))
    allow(delegate).to receive(:filesystem) { filesystem }
    delegate.stub_chain(:configuration, :hive).and_return(configuration)
  end

  describe '#stdin' do
    context 'with exec' do
      let(:attrs) { {exec: %q(SELECT * FROM table;)} }
      subject(:stdin) { instance.stdin }

      it { is_expected.to be_a(StringIO) }

      describe '#string' do
        subject { stdin.string }
        it { is_expected.to eq(%q(SELECT * FROM table;)) }
      end
    end
  end

  describe '#command_args' do
    let(:default_command) { ['hive', '--database', 'default'] }

    subject do
      instance.command_args
    end

    it { is_expected.to eq(default_command) }

    context 'with command attrs' do
      let(:options) { [{'-d' => 'DATABASE=development'}] }
      it { is_expected.to eq([*default_command, '-d', 'DATABASE=development']) }
    end

    context 'with file' do
      let(:attrs) { {file: 'zomg.hql'} }
      it { is_expected.to eq([*default_command, '-f', 'zomg.hql']) }
    end

    context 'with variables' do
      let(:attrs) { {file: 'zomg.hql', variables: {R: 'R2DO', C: 'C3PO'}} }
      it { is_expected.to eq([*default_command, '-f', 'zomg.hql', '-d', 'R=R2DO', '-d', 'C=C3PO']) }
    end

    context 'with setup files' do
      before do
        filesystem.touch!('setup_a.hql', 'setup_b.hql')
      end

      let(:attrs) { {setup_files: ['setup_a.hql', 'setup_b.hql']} }
      it { is_expected.to eq([*default_command, '-i', 'setup_a.hql', '-i', 'setup_b.hql']) }
    end

    context 'with schema files' do
      before do
        filesystem.touch!('schema_a.hql', 'schema_b.hql')
      end

      let(:attrs) { {schema_files: ['schema_a.hql', 'schema_b.hql']} }
      it { is_expected.to eq([*default_command, '-i', 'schema_a.hql', '-i', 'schema_b.hql']) }
    end

    context 'with schema files that are globs' do
      before do
        filesystem.touch!('schema_a.hql', 'schema_b.hql')
      end

      let(:attrs) { {schema_files: ['schema*.hql']} }
      it { is_expected.to eq([*default_command, '-i', 'schema_a.hql', '-i', 'schema_b.hql']) }
    end

    context 'with template file' do
      let(:attrs) { {file: 'zomg.hql.erb'} }
      before do
        expect(Masamune::Template).to receive(:render_to_file).with('zomg.hql.erb', {}).and_return('zomg.hql')
        expect_any_instance_of(Masamune::Filesystem).to receive(:copy_file)
      end
      it { is_expected.to eq([*default_command, '-f', filesystem.get_path(:tmp_dir, 'zomg.hql')]) }
    end
  end

  describe '#handle_stdout' do
    let(:buffer) { StringIO.new }
    let(:delimiter) { "\t" }
    let(:attrs) { {buffer: buffer, delimiter: delimiter, csv: true} }
    let(:input_row) { ['A', 'NULL', 'B', 'C', '', 'E'].join(delimiter) }
    let(:output_row) { ['A', nil, 'B', 'C', nil, 'E'].join(',') }

    before do
      instance.handle_stdout(input_row, 0)
    end

    it { expect(buffer.string).to eq(output_row + "\n") }
  end
end
