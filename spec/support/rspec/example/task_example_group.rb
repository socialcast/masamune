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

module TaskExampleGroup
  def capture(stdout, stderr, &block)
    tmp_stdout, $stdout = $stdout, stdout
    tmp_stderr, $stderr = $stderr, stderr
    yield
  ensure
    $stdout, $stderr = tmp_stdout, tmp_stderr
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

  shared_examples 'command usage' do
    it 'exits with status code 0 and prints command usage' do
      expect { cli_invocation }.to raise_error { |e|
        expect(e).to be_a(SystemExit)
        expect(e.status).to eq(0)
      }
      expect(stdout.string).to match(/^Usage:/)
      expect(stdout.string).to match(/^Options:/)
      expect(stderr.string).to be_blank
    end
  end

  shared_examples 'executes with success' do
    it 'exits with status code 0' do
      expect { cli_invocation }.to raise_error { |e|
        expect(e).to be_a(SystemExit)
        expect(e.status).to eq(0)
      }
    end
  end

  shared_examples 'raises Thor::MalformattedArgumentError' do |message|
    it { expect { cli_invocation }.to raise_error Thor::MalformattedArgumentError, message }
  end

  def self.included(base)
    base.before :all do
      ENV['THOR_DEBUG'] = '1'
    end

    base.let(:thor_class) { described_class }
    base.let(:command) { nil }
    base.let(:options) { {} }
    base.let!(:stdout) { StringIO.new }
    base.let!(:stderr) { StringIO.new }

    base.before do
      thor_class.send(:include, Masamune::ThorMute)
    end

    base.subject(:cli_invocation) do
      capture(stdout, stderr) do
        thor_class.start([command, *options].compact)
      end
    end
  end
end

RSpec.configure do |config|
  config.include TaskExampleGroup, type: :task, file_path: %r{spec/masamune/(.*?/)?.*thor_spec\.rb}
end
