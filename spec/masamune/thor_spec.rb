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

  let(:instance) do
    Class.new(Thor) do
      include Masamune::Thor
      include Masamune::Actions::DataFlow

      desc 'command', 'command'
      target "target/%Y-%m-%d", :for => :command_task
      source "source/%Y%m%d*.log", :wildcard => true, :for => :command_task
      def command_task
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
        instance.start([command, *options].compact)
      end
    end

    context 'without command' do
      it do
        expect { subject }.to raise_error SystemExit
        stdout.string.should =~ /^Commands:/
        stderr.string.should be_blank
      end
    end

    context 'with command and no input options' do
      let(:command) { 'command' }
      it { expect { subject }.to raise_error Thor::RequiredArgumentMissingError, /No value provided for required options '--start'/ }
    end

    context 'with command and --start and matching targets' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      it do
        expect { subject }.to raise_error SystemExit
        stdout.string.should be_blank
        stderr.string.should =~ /\ANo matching missing targets/
      end
    end

    context 'with command and invalid --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', 'xxx'] }
      it { expect { subject }.to raise_error Thor::MalformattedArgumentError, /Expected date time value for '--start'; got/ }
    end
  end
end
