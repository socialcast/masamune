require 'spec_helper'
require 'thor'

ENV['THOR_DEBUG'] = '1'
describe Masamune::Thor do
  def capture(stdout, stderr, &block)
    tmp_stdout, $stdout = $stdout, stdout
    tmp_stderr, $stderr = $stderr, stderr
    yield
  ensure
    $stdout, $stderr = tmp_stdout, tmp_stderr
  end

  let(:klass) do
    Class.new(Thor) do
      include Masamune::Thor
      include Masamune::Actions::DataFlow
      include Masamune::ThorMute

      desc 'command', 'command'
      target path: "target/%Y-%m-%d"
      source path: "source/%Y%m%d*.log"
      method_option :zombo, desc: 'Anything is possible'
      def command
        # NOP
      end

      desc 'other', 'other'
      skip
      def other
        # NOP
      end
    end
  end

  before do
    allow_any_instance_of(klass).to receive(:top_level?).and_return(true)
  end

  context 'CLI' do
    let(:command) { nil }
    let(:options) { {} }

    let!(:stdout) { StringIO.new }
    let!(:stderr) { StringIO.new }

    subject(:cli_invocation) do
      capture(stdout, stderr) do
        klass.start([command, *options].compact)
      end
    end

    context 'ouside of top level' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }

      before do
        allow_any_instance_of(klass).to receive(:top_level?).and_return(false)
      end

      it 'continues execution' do
        expect { cli_invocation }.to_not raise_error
      end
    end

    shared_examples 'general usage' do
      it 'exits with status code 0 and prints general usage' do
        expect { cli_invocation }.to raise_error { |e|
          expect(e).to be_a(SystemExit)
          expect(e.status).to eq(0)
        }
        expect(stdout.string).to match(/^Commands:/)
        expect(stderr.string).to be_blank
      end
    end

    context 'without command' do
      it_behaves_like 'general usage'
    end

    context 'with help command ' do
      let(:command) { 'help' }
      it_behaves_like 'general usage'
    end

    shared_examples 'command usage' do
      it 'exits with status code 0 and prints command usage' do
        expect { cli_invocation }.to raise_error { |e|
          expect(e).to be_a(SystemExit)
          expect(e.status).to eq(0)
        }
        expect(stdout.string).to match(/^Usage:/)
        expect(stdout.string).to match(/--zombo/)
        expect(stderr.string).to be_blank
      end
    end

    context 'with --help option' do
      let(:command) { 'command' }
      let(:options) { ['--help'] }
      it_behaves_like 'command usage'
    end

    context 'with help subcommand ' do
      let(:command) { 'help' }
      let(:options) { ['command'] }
      it_behaves_like 'command usage'
    end

    context 'with command and --version' do
      let(:command) { 'command' }
      let(:options) { ['--version'] }
      it 'exits with status code 0 and prints version' do
        expect { cli_invocation }.to raise_error { |e|
          expect(e).to be_a(SystemExit)
          expect(e.status).to eq(0)
        }
        expect(stdout.string).to match(/\Amasamune/)
        expect(stderr.string).to be_blank
      end
    end

    context 'with command and no input options' do
      let(:command) { 'command' }
      it { expect { cli_invocation }.to raise_error Thor::RequiredArgumentMissingError, /No value provided for required options '--start'/ }
    end

    context 'with command and invalid --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', 'xxx'] }
      it { expect { cli_invocation }.to raise_error Thor::MalformattedArgumentError, /Expected date time value for '--start'; got/ }
    end

    context 'with command and invalid --stop' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01', '--stop', 'xxx'] }
      it { expect { cli_invocation }.to raise_error Thor::MalformattedArgumentError, /Expected date time value for '--stop'; got/ }
    end

    context 'with command and invalid --sources' do
      let(:command) { 'command' }
      let(:options) { ['--sources', 'foo'] }
      it { expect { cli_invocation }.to raise_error Thor::MalformattedArgumentError, /Expected file value for '--sources'; got/ }
    end

    context 'with command and invalid --targets' do
      let(:command) { 'command' }
      let(:options) { ['--targets', 'foo'] }
      it { expect { cli_invocation }.to raise_error Thor::MalformattedArgumentError, /Expected file value for '--targets'; got/ }
    end

    context 'with command and both --sources and --targets' do
      let(:command) { 'command' }
      let(:options) { ['--sources', 'sources', '--targets', 'targets'] }
      it { expect { cli_invocation }.to raise_error Thor::MalformattedArgumentError, /Cannot specify both option '--sources' and option '--targets'/ }
    end

    context 'with command and --start and bad --config file' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01', '--config', 'xxx'] }
      it { expect { cli_invocation }.to raise_error Thor::MalformattedArgumentError, /Could not load file provided for '--config'/ }
    end

    context 'with command and --start and missing system --config file' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        expect_any_instance_of(Masamune::Filesystem).to receive(:resolve_file)
      end
      it { expect { cli_invocation }.to raise_error Thor::RequiredArgumentMissingError, /Option --config or valid system configuration file required/ }
    end

    context 'with command and -- --extra --args' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01', '--', '--extra', '--args'] }
      before do
        expect_any_instance_of(klass).to receive(:extra=).with(['--extra', '--args'])
      end
      it do
        expect { cli_invocation }.to raise_error SystemExit
      end
    end

    context 'with command and --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      it 'exits with status code 0 without error message' do
        expect { cli_invocation }.to raise_error { |e|
          expect(e).to be_a(SystemExit)
          expect(e.status).to eq(0)
        }
        expect(stdout.string).to match(/\AUsing '.*' for --start/)
        expect(stderr.string).to eq('')
      end
    end

    context 'with command and natural language --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', 'yesterday'] }
      it 'exits with status code  0 without error message' do
        expect { cli_invocation }.to raise_error { |e|
          expect(e).to be_a(SystemExit)
          expect(e.status).to eq(0)
        }
        expect(stdout.string).to match(/\AUsing '.*' for --start/)
        expect(stderr.string).to eq('')
      end
    end

    context 'with command that raises exception before initialization' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        expect_any_instance_of(Logger).to receive(:error).with(/random exception/)
        allow(klass).to receive(:dispatch).and_raise('random exception')
      end
      it { expect { cli_invocation }.to raise_error /random exception/ }
    end

    context 'with command that raises exception after initialization' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        expect_any_instance_of(Logger).to receive(:error).with(/random exception/)
        allow(klass).to receive(:after_initialize_invoke).and_raise('random exception')
      end
      it { expect { cli_invocation }.to raise_error /random exception/ }
    end
  end

  context '.parse_extra' do
    subject do
      klass.parse_extra(argv)
    end

    context 'without --' do
      let(:argv) { ['--flag', 'true'] }
      it { is_expected.to eq([['--flag', 'true'],[]]) }
    end

    context 'with -- and no following arguments' do
      let(:argv) { ['--flag', 'true', '--'] }
      it { is_expected.to eq([['--flag', 'true'],[]]) }
    end

    context 'with -- and a single extra argument' do
      let(:argv) { ['--flag', 'true', '--', '--more'] }
      it { is_expected.to eq([['--flag', 'true'], ['--more']]) }
    end

    context 'with -- and multiple extra agruments' do
      let(:argv) { ['--flag', 'true', '--', '--more', 'flag'] }
      it { is_expected.to eq([['--flag', 'true'], ['--more', 'flag']]) }
    end

    context 'with leading -- and a single extra argument' do
      let(:argv) { ['--', '--more'] }
      it { is_expected.to eq([[], ['--more']]) }
    end
  end
end
