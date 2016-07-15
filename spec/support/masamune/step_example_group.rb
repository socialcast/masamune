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

module Masamune::StepExampleGroup
  extend ActiveSupport::Concern

  include Masamune::SharedExampleGroup

  shared_context 'step_fixture' do |context_options = {}|
    fixture_file = example_fixture_file(context_options.slice(:fixture, :file, :path))
    step_file = example_step

    args = context_options[:args]
    subject do
      capture_popen([step_file, args].compact.join(' '), input)
    end

    context "with #{fixture_file} fixture" do
      let(:fixture) { Masamune::StepFixture.load({ file: fixture_file }, binding) }

      let(:input) { fixture.input }
      let(:output) { fixture.output }

      it 'should match output' do
        is_expected.to eq(output)
      end

      after(:each) do |example|
        if example.exception && ENV['MASAMUNE_RECORD']
          shell = Thor::Shell::Basic.new
          shell.say(example.exception)
          if shell.yes?('Save recording?')
            fixture.output = subject
            fixture.save
          end
        end
      end
    end
  end
end

RSpec.configure do |config|
  config.include Masamune::StepExampleGroup, type: :step, file_path: %r{.*/spec/.*step_spec\.rb}
  config.include Masamune::StepExampleGroup, type: :step, file_path: %r{.*/spec/.*mapper_spec\.rb}
  config.include Masamune::StepExampleGroup, type: :step, file_path: %r{.*/spec/.*reducer_spec\.rb}
end
