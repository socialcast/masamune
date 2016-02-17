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

$: << File.join(File.dirname(__FILE__), '..', '..')

require 'apache_log/task'

describe ApacheLogTask do
  describe 'load_users' do
    include_context 'task_fixture', fixture: 'load_users' do
      let(:command) { 'load_users' }
      let(:options) { ['--start', '2015-10-01', '--stop', '2015-10-02'] }
    end
  end

  describe 'extract_logs' do
    include_context 'task_fixture', fixture: 'extract_logs' do
      let(:command) { 'extract_logs' }
      let(:options) { ['--start', '2015-10-01', '--stop', '2015-10-02'] }
    end
  end

  describe 'load_visits' do
    include_context 'task_fixture', fixture: 'load_visits', execute_command: false do
      let(:command) { 'load_visits' }
      let(:options) { ['--start', '2015-10-01', '--stop', '2015-10-02'] }
      before do
        execute_command
      end
    end
  end
end
