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

module Masamune::TaskExampleGroup
  module TaskFixtureContent
    def self.included(base)
      base.let!(:default_options) { configuration.as_options }

      base.let(:thor_class) { described_class }
      base.let(:command) { nil }
      base.let(:options) { {} }
      base.let!(:stdout) { StringIO.new }
      base.let!(:stderr) { StringIO.new }

      base.let(:execute_command_times) { 1 }

      base.subject(:execute_command) do
        capture(stdout, stderr, enable: !default_options.include?('--debug')) do
          execute_command_times.times do
            Array.wrap(command).each do |cmd|
              described_class.start([cmd, *(default_options + options)].compact)
            end
          end
        end
      end
    end

    shared_context 'task_fixture' do |context_options = {}|
      include_context 'job_fixture', context_options

      let(:execute_command_times) { !ENV['MASAMUNE_FASTER_SPEC'] && context_options.fetch(:idempotent, false) ? 2 : 1 }
    end
  end

  def self.included(base)
    base.send(:include, Masamune::ExampleGroup)
    base.send(:include, Masamune::Actions::Filesystem)
    base.send(:include, Masamune::Actions::Hive)
    base.send(:include, Masamune::Transform::DenormalizeTable)
    base.send(:include, TaskFixtureContent)
  end
end

RSpec.configure do |config|
  config.include Masamune::TaskExampleGroup, type: :task, file_path: %r{.*/spec/.*task_spec\.rb}
end
