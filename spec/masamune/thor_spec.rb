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
      def command
        # NOP
      end
    end
  end

  before do
    klass.any_instance.stub(:top_level?).and_return(true)
  end

  context 'CLI' do
    let(:command) { nil }
    let(:options) { {} }

    let!(:stdout) { StringIO.new }
    let!(:stderr) { StringIO.new }

    subject do
      capture(stdout, stderr) do
        klass.start([command, *options].compact)
      end
    end

    context 'ouside of top level' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }

      before do
        klass.any_instance.stub(:top_level?).and_return(false)
      end

      it 'continues execution' do
        expect { subject }.to_not raise_error
      end
    end

    context 'without command' do
      it 'exits with status code 0 and prints usage' do
        expect { subject }.to raise_error { |e|
          e.should be_a(SystemExit)
          e.status.should == 0
        }
        stdout.string.should =~ /^Commands:/
        stderr.string.should be_blank
      end
    end

    context 'with command and --version' do
      let(:command) { 'command' }
      let(:options) { ['--version'] }
      it 'exits with status code 0 and prints version' do
        expect { subject }.to raise_error { |e|
          e.should be_a(SystemExit)
          e.status.should == 0
        }
        stdout.string.should =~ /\Amasamune/
        stderr.string.should be_blank
      end
    end

    context 'with command and no input options' do
      let(:command) { 'command' }
      it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /No value provided for required options '--start'/ }
    end

    context 'with command and invalid --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', 'xxx'] }
      it { expect { subject }.to raise_error Thor::MalformattedArgumentError, /Expected date time value for '--start'; got/ }
    end

    context 'with command and invalid --stop' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01', '--stop', 'xxx'] }
      it { expect { subject }.to raise_error Thor::MalformattedArgumentError, /Expected date time value for '--stop'; got/ }
    end

    context 'with command and invalid --sources' do
      let(:command) { 'command' }
      let(:options) { ['--sources', 'foo'] }
      it { expect { subject }.to raise_error Thor::MalformattedArgumentError, /Expected file value for '--sources'; got/ }
    end

    context 'with command and invalid --targets' do
      let(:command) { 'command' }
      let(:options) { ['--targets', 'foo'] }
      it { expect { subject }.to raise_error Thor::MalformattedArgumentError, /Expected file value for '--targets'; got/ }
    end

    context 'with command and both --sources and --targets' do
      let(:command) { 'command' }
      let(:options) { ['--sources', 'sources', '--targets', 'targets'] }
      it { expect { subject }.to raise_error Thor::MalformattedArgumentError, /Cannot specify both option '--sources' and option '--targets'/ }
    end

    context 'with command and --start and bad --config file' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01', '--config', 'xxx'] }
      it { expect { subject }.to raise_error Thor::MalformattedArgumentError, /Could not load file provided for '--config'/ }
    end

    context 'with command and --start and missing system --config file' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        Masamune::Filesystem.any_instance.should_receive(:resolve_file)
      end
      it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /Option --config or valid system configuration file required/ }
    end

    context 'with command and -- --extra --args' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01', '--', '--extra', '--args'] }
      before do
        klass.any_instance.should_receive(:extra=).with(['--extra', '--args'])
      end
      it do
        expect { subject }.to raise_error SystemExit
      end
    end

    context 'with command and --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      it 'exits with status code 0 without error message' do
        expect { subject }.to raise_error { |e|
          e.should be_a(SystemExit)
          e.status.should == 0
        }
        stdout.string.should =~ /\AUsing '.*' for --start/
        stderr.string.should == ''
      end
    end

    context 'with command and natural language --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', 'yesterday'] }
      it 'exits with status code  0 without error message' do
        expect { subject }.to raise_error { |e|
          e.should be_a(SystemExit)
          e.status.should == 0
        }
        stdout.string.should =~ /\AUsing '.*' for --start/
        stderr.string.should == ''
      end
    end

    context 'with command that raises exception' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        Masamune.logger.should_receive(:error).with('random exception')
        klass.stub(:dispatch).and_raise('random exception')
      end
      it { expect { subject }.to raise_error /random exception/ }
    end
  end

  context '.parse_extra' do
    subject do
      klass.parse_extra(argv)
    end

    context 'without --' do
      let(:argv) { ['--flag', 'true'] }
      it { should == [['--flag', 'true'],[]] }
    end

    context 'with -- and no following arguments' do
      let(:argv) { ['--flag', 'true', '--'] }
      it { should == [['--flag', 'true'],[]] }
    end

    context 'with -- and a single extra argument' do
      let(:argv) { ['--flag', 'true', '--', '--more'] }
      it { should == [['--flag', 'true'], ['--more']] }
    end

    context 'with -- and multiple extra agruments' do
      let(:argv) { ['--flag', 'true', '--', '--more', 'flag'] }
      it { should == [['--flag', 'true'], ['--more', 'flag']] }
    end

    context 'with leading -- and a single extra argument' do
      let(:argv) { ['--', '--more'] }
      it { should == [[], ['--more']] }
    end
  end
end
