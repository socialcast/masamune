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

require_relative 'shared_example_group'

module Masamune::TaskExampleGroup
  extend ActiveSupport::Concern

  include Masamune::ExampleGroup
  include Masamune::SharedExampleGroup
  include Masamune::Actions::Filesystem
  include Masamune::Actions::Hive
  include Masamune::Transform::DenormalizeTable

  shared_examples 'general usage' do
    it 'exits with status code 0 and prints general usage' do
      expect { execute_command }.to raise_error { |e|
        expect(e).to be_a(SystemExit)
        expect(e.status).to eq(0)
      }
      expect(stdout.string).to match(/^Commands:/)
      expect(stderr.string).to be_blank
    end
  end

  shared_examples 'command usage' do
    it 'exits with status code 0 and prints command usage' do
      expect { execute_command }.to raise_error { |e|
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
      expect { execute_command }.to raise_error { |e|
        expect(e).to be_a(SystemExit)
        expect(e.status).to eq(0)
      }
    end
  end

  shared_examples 'raises Thor::MalformattedArgumentError' do |message|
    it { expect { execute_command }.to raise_error Thor::MalformattedArgumentError, message }
  end

  shared_examples 'raises Thor::RequiredArgumentMissingError' do |message|
    it { expect { execute_command }.to raise_error Thor::RequiredArgumentMissingError, message }
  end

  shared_context 'task_fixture' do |context_options = {}|
    include_context 'job_fixture', context_options

    let(:execute_command_times) { !ENV['MASAMUNE_FASTER_SPEC'] && context_options.fetch(:idempotent, false) ? 2 : 1 }
  end

  included do
    let!(:default_options) { configuration.as_options }

    let(:thor_class) { described_class }
    let(:command) { nil }
    let(:options) { [] }
    let!(:stdout) { StringIO.new }
    let!(:stderr) { StringIO.new }

    let(:execute_command_times) { 1 }

    before do
      thor_class.send(:include, Masamune::ThorMute)
    end

    subject(:execute_command) do
      capture(stdout: stdout, stderr: stderr, enable: !default_options.include?('--debug')) do
        execute_command_times.times do
          thor_class.start([command, *(default_options + options)].compact)
        end
      end
    end
  end
end

RSpec.configure do |config|
  config.include Masamune::TaskExampleGroup, type: :task, file_path: %r{.*/spec/.*task_spec\.rb}
end
