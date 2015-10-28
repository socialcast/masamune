# require 'spec_helper'
$: << File.join(File.dirname(__FILE__), '..', '..')
require 'apache_log/task'

describe ApacheLogTask do
  describe 'extract_logs' do
    include_context 'task_fixture' do
      let(:command) { 'extract_logs' }
      let(:options) { ['--start', '2015-10-01', '--stop', '2015-10-02'] }
      before do
        execute_command
      end
    end
  end
end
