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

module Masamune::JobExampleGroup
  extend ActiveSupport::Concern

  include Masamune::ExampleGroup
  include Masamune::SharedExampleGroup
  include Masamune::Actions::Filesystem
  include Masamune::Actions::Hive
  include Masamune::Actions::Postgres

  shared_context 'job_fixture' do |context_options = {}|
    fixture_file = example_fixture_file(context_options.slice(:fixture, :file, :path))
    let(:fixture) { example_fixture(file: fixture_file) }

    before :all do
      load_example_config!
      clean_example_run!
    end

    before do
      setup_example_input!(fixture)
    end

    it "should match #{fixture_file}" do
      aggregate_failures 'generates expected output' do
        gather_example_output(fixture) do |actual_data, expect_file, expect_data|
          expect(File.exist?(expect_file)).to eq(true)
          expect(actual_data).to eq(expect_data)
        end
      end
    end
  end
end

RSpec.configure do |config|
  config.include Masamune::JobExampleGroup, type: :job, file_path: %r{.*/spec/.*job_spec\.rb}
  config.include Masamune::JobExampleGroup, type: :task, file_path: %r{.*/spec/.*task_spec\.rb}
end
