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

describe Masamune::Thor do
  let(:thor_class) do
    Class.new(Thor) do
      include Masamune::Thor
      include Masamune::Actions::DataFlow

      namespace :example

      desc 'command', 'command'
      target path: fs.path(:tmp_dir, 'target/%Y-%m-%d')
      source path: fs.path(:tmp_dir, 'source/%Y%m%d*.log')
      def command_task
        # NOP
      end

      desc 'other', 'other'
      skip
      def other_task
        # NOP
      end

      desc 'unknown', 'unknown'
      target path: fs.path(:unknown_dir, 'target/%Y-%m-%d')
      source path: fs.path(:unknown_dir, 'source/%Y%m%d*.log')
      def unknown_task
        # NOP
      end
    end
  end

  before do
    allow_any_instance_of(thor_class).to receive(:top_level?).and_return(true)
  end

  context 'CLI' do
    context 'ouside of top level' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }

      before do
        allow_any_instance_of(thor_class).to receive(:top_level?).and_return(false)
      end

      it 'continues execution' do
        expect { execute_command }.to_not raise_error
      end
    end

    context 'without command' do
      it_behaves_like 'general usage'
    end

    context 'with help command ' do
      let(:command) { 'help' }
      it_behaves_like 'general usage'
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
        expect { execute_command }.to raise_error { |e|
          expect(e).to be_a(SystemExit)
          expect(e.message).to eq('exit')
          expect(e.status).to eq(0)
        }
        expect(stdout.string).to match(/\Amasamune/)
        expect(stderr.string).to be_blank
      end
    end

    context 'with command and no input options' do
      let(:command) { 'command' }
      it_behaves_like 'raises Thor::RequiredArgumentMissingError', /No value provided for required options '--start'/
    end

    context 'with command and invalid --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', 'xxx'] }
      it_behaves_like 'raises Thor::MalformattedArgumentError', /Expected date time value for '--start'; got/
    end

    context 'with command and invalid --stop' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01', '--stop', 'xxx'] }
      it_behaves_like 'raises Thor::MalformattedArgumentError', /Expected date time value for '--stop'; got/
    end

    context 'with command and invalid --sources' do
      let(:command) { 'command' }
      let(:options) { ['--sources', 'foo'] }
      it_behaves_like 'raises Thor::MalformattedArgumentError', /Expected file value for '--sources'; got/
    end

    context 'with command and invalid --targets' do
      let(:command) { 'command' }
      let(:options) { ['--targets', 'foo'] }
      it_behaves_like 'raises Thor::MalformattedArgumentError', /Expected file value for '--targets'; got/
    end

    context 'with command and both --sources and --targets' do
      let(:command) { 'command' }
      let(:options) { ['--sources', 'sources', '--targets', 'targets'] }

      it_behaves_like 'raises Thor::MalformattedArgumentError', /Cannot specify both option '--sources' and option '--targets'/
    end

    context 'with command and --start and bad --config file' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01', '--config', 'xxx'] }
      it_behaves_like 'raises Thor::MalformattedArgumentError', /Could not load file provided for '--config'/
    end

    context 'with command and --start and missing system --config file' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        expect_any_instance_of(Masamune::Filesystem).to receive(:resolve_file)
      end
      it_behaves_like 'raises Thor::RequiredArgumentMissingError', /Option --config or valid system configuration file required/
    end

    context 'with command and -- --extra --args' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01', '--', '--extra', '--args'] }
      before do
        expect_any_instance_of(thor_class).to receive(:extra=).with(['--extra', '--args'])
      end
      it_behaves_like 'executes with success'
    end

    context 'with command and --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      it_behaves_like 'executes with success'
    end

    context 'with command and natural language --start' do
      let(:command) { 'command' }
      let(:options) { ['--start', 'yesterday'] }
      it_behaves_like 'executes with success'
    end

    context 'with command that raises exception before initialization' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        expect_any_instance_of(Logger).to receive(:error).with(/random exception/)
        allow(thor_class).to receive(:dispatch).and_raise('random exception')
      end
      it { expect { execute_command }.to raise_error(/random exception/) }
    end

    context 'with command that raises exception after initialization' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        expect_any_instance_of(Logger).to receive(:error).with(/random exception/)
        allow(thor_class).to receive(:after_initialize_invoke).and_raise('random exception')
      end
      it { expect { execute_command }.to raise_error(/random exception/) }
    end

    context 'with command that raises exception during execution' do
      let(:command) { 'unknown' }
      let(:options) { ['--start', '2013-01-01'] }
      it 'exits with status code 1 and prints error to stderr' do
        expect { execute_command }.to raise_error { |e|
          expect(e).to be_a(SystemExit)
          expect(e.message).to eq('Path :unknown_dir not defined')
          expect(e.status).to eq(1)
        }
        expect(stdout.string).to be_blank
        expect(stderr.string).to match(/Path :unknown_dir not defined/)
      end
    end
  end

  context 'with command that prints :current_dir' do
    let(:thor_class) do
      Class.new(Thor) do
        include Masamune::Thor

        desc 'current_dir', 'current_dir'
        def current_dir_task
          console(fs.path(:current_dir))
        end
      end
    end

    let(:command) { 'current_dir' }
    let(:options) { ['--no-quiet'] }
    it 'prints :current_dir' do
      execute_command
      expect(stdout.string).to eq(File.dirname(__FILE__) + "\n")
      expect(stderr.string).to be_blank
    end
  end

  context '.parse_extra' do
    subject do
      thor_class.parse_extra(argv)
    end

    context 'without --' do
      let(:argv) { ['--flag', 'true'] }
      it { is_expected.to eq([['--flag', 'true'], []]) }
    end

    context 'with -- and no following arguments' do
      let(:argv) { ['--flag', 'true', '--'] }
      it { is_expected.to eq([['--flag', 'true'], []]) }
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

  context '#qualify_task_name' do
    let(:thor_class) do
      Class.new(Thor) do
        include Masamune::Thor
      end
    end

    let(:instance) { thor_class.new([], {}, {}) }

    before do
      expect(instance).to receive(:current_namespace).at_most(:once).and_return('namespace')
    end

    subject do
      instance.qualify_task_name(name)
    end

    context 'without namespace' do
      let(:name) { 'other' }
      it { is_expected.to eq('namespace:other') }
    end

    context 'with namespace' do
      let(:name) { 'namespace:other' }
      it { is_expected.to eq('namespace:other') }
    end

    context 'with task suffix' do
      let(:name) { 'namespace:other_task' }
      it { is_expected.to eq('namespace:other') }
    end
  end
end
