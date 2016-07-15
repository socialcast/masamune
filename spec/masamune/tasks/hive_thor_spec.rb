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

require 'masamune/tasks/hive_thor'

describe Masamune::Tasks::HiveThor do
  context 'with help command ' do
    let(:command) { 'help' }
    before do
      expect_any_instance_of(described_class).to receive(:hive).never
    end
    it_behaves_like 'command usage'
  end

  context 'without arguments' do
    let(:options) { [] }

    it do
      expect_any_instance_of(described_class).to receive(:hive).with(hash_including(retries: 0)).once.and_return(mock_success)
      execute_command
    end
  end

  context 'with --file and --initialize' do
    let(:options) { ['--file=zombo.hql', '--initialize'] }
    it do
      expect_any_instance_of(described_class).to receive(:hive).with(exec: 'CREATE DATABASE IF NOT EXISTS masamune;', database: nil).and_return(mock_success)
      expect_any_instance_of(described_class).to receive(:hive).with(file: instance_of(String)).and_return(mock_success)
      expect_any_instance_of(described_class).to receive(:hive).with(hash_including(file: File.expand_path('zombo.hql'))).once.and_return(mock_success)
      execute_command
    end
  end

  context 'with --file' do
    let(:options) { ['--file=zombo.hql'] }
    it do
      expect_any_instance_of(described_class).to receive(:hive).with(hash_including(file: File.expand_path('zombo.hql'))).once.and_return(mock_success)
      execute_command
    end
  end

  context 'with --output' do
    let(:options) { ['--output=report.txt'] }
    it do
      expect_any_instance_of(described_class).to receive(:hive).with(hash_including(output: File.expand_path('report.txt'))).once.and_return(mock_success)
      execute_command
    end
  end

  context 'with --variables=YEAR:2015 MONTH:1' do
    let(:options) { ['--variables=YEAR:2015', 'MONTH:1'] }
    it do
      expect_any_instance_of(described_class).to receive(:hive).with(hash_including(variables: { 'YEAR' => '2015', 'MONTH' => '1' })).once.and_return(mock_success)
      execute_command
    end
  end

  context 'with -X YEAR:2015 MONTH:1' do
    let(:options) { ['-X', 'YEAR:2015', 'MONTH:1'] }
    it do
      expect_any_instance_of(described_class).to receive(:hive).with(hash_including(variables: { 'YEAR' => '2015', 'MONTH' => '1' })).once.and_return(mock_success)
      execute_command
    end
  end

  context 'with --retry' do
    let(:options) { ['--retry'] }
    it do
      expect_any_instance_of(described_class).to receive(:hive).with(hash_excluding(retries: 0)).once.and_return(mock_success)
      execute_command
    end
  end
end
