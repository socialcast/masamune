require 'spec_helper'
require 'thor'

ENV['THOR_DEBUG'] = '1'
describe Masamune::Thor do
  let(:client) { Masamune::Client.new }

  before do
    Masamune.client = client
  end

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

      desc 'command', 'command'
      target "target/%Y-%m-%d"
      source "source/%Y%m%d*.log", :wildcard => true
      def command
        # NOP
      end
    end
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

    context 'with command and --dry_run' do
      let(:command) { 'command' }
      let(:options) { ['--dry_run'] }
      before do
        klass.any_instance.should_receive(:hive).with(exec: 'show tables;', safe: true, fail_fast: false).and_return(mock(success?: false))
      end
      it { expect { subject }.to raise_error Thor::InvocationError, /Dry run of hive failed/ }
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

    context 'with command and --start and no matching targets' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      it 'exits with status code 1 and prints error message' do
        expect { subject }.to raise_error { |e|
          e.should be_a(SystemExit)
          e.status.should == 1
        }
        stdout.string.should =~ /\AUsing '.*' for --start/
        stderr.string.should =~ /\ANo matching missing targets/
      end
    end

    context 'with command and natural language --start and no matching targets' do
      let(:command) { 'command' }
      let(:options) { ['--start', 'yesterday'] }
      it 'exits with status code 1 and prints error message' do
        expect { subject }.to raise_error { |e|
          e.should be_a(SystemExit)
          e.status.should == 1
        }
        stdout.string.should =~ /\AUsing '.*' for --start/
        stderr.string.should =~ /\ANo matching missing targets/
      end
    end

    context 'when lock cannot be acquired' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        klass.any_instance.should_receive(:acquire_lock).and_return(false)
      end
      it 'exits with status code 1 and prints error message' do
        expect { subject }.to raise_error { |e|
          e.should be_a(SystemExit)
          e.status.should == 1
        }
        stdout.string.should =~ /\AUsing '.*' for --start/
        stderr.string.should =~ /\AAnother process is already running/
      end
    end

    context 'when elastic_mapreduce is enabled' do
      before do
        Masamune::Configuration.any_instance.stub(:elastic_mapreduce_enabled?) { true }
      end

      context 'with command and --start and no --jobflow' do
        let(:command) { 'command' }
        let(:options) { ['--start', '2013-01-01'] }
        it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /No value provided for required options '--jobflow'/ }
      end

      context 'with command and --start and invalid --jobflow' do
        let(:command) { 'command' }
        let(:options) { ['--start', '2013-01-01', '--jobflow', 'xxx'] }
        before do
          klass.any_instance.should_receive(:elastic_mapreduce).with(extra: '--list', jobflow: 'xxx', fail_fast: false).and_return(mock(success?: false))
        end
        it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /'--jobflow' doesn't exist/ }
      end
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
