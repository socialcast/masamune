#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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

require 'spec_helper'
require 'thor'

describe Masamune::Thor do
  let(:thor_class) do
    Class.new(Thor) do
      include Masamune::Thor
      include Masamune::Actions::DataFlow

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
        expect { cli_invocation }.to_not raise_error
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
        expect_any_instance_of(thor_class).to receive(:extra=).with(['--extra', '--args'])
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
        allow(thor_class).to receive(:dispatch).and_raise('random exception')
      end
      it { expect { cli_invocation }.to raise_error /random exception/ }
    end

    context 'with command that raises exception after initialization' do
      let(:command) { 'command' }
      let(:options) { ['--start', '2013-01-01'] }
      before do
        expect_any_instance_of(Logger).to receive(:error).with(/random exception/)
        allow(thor_class).to receive(:after_initialize_invoke).and_raise('random exception')
      end
      it { expect { cli_invocation }.to raise_error /random exception/ }
    end
  end

  context '.parse_extra' do
    subject do
      thor_class.parse_extra(argv)
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
