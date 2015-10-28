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

module Masamune::TaskExampleGroup
  module TaskFixtureContent
    shared_context 'task_fixture' do |context_options = {}|
      include_context 'job_fixture', context_options
      let!(:default_options) { configuration.as_options }

      let(:stdout) { @stdout }
      let(:stderr) { @stderr }
      let(:status) { @status }

      let(:command) { nil }
      let(:options) { [] }

      subject(:execute_command) do
        n = context_options.fetch(:idempotent, false) ? 2 : 1
        n = 1 if ENV['MASAMUNE_FASTER_SPEC']
        capture(!default_options.include?('--debug')) do
          n.times do
            Array.wrap(command).each do |cmd|
              described_class.start([cmd, *(default_options + options)].compact)
            end
          end
        end
      end
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
  config.include Masamune::TaskExampleGroup, :type => :task, :file_path => %r{.*/spec/.*task_spec\.rb}
end
