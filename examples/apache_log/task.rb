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

$: << File.dirname(__FILE__)

require 'random_apache_log_generator'

class ApacheLogsTask < Thor
  include Masamune::Thor
  include Masamune::Actions::DataFlow

  namespace :examples
  class_option :config, :desc => 'Configuration file', :default => fs.get_path(:current_dir, 'config.yml.erb')

  desc 'generate_sample_logs', 'Generate sample Apache log files'
  source none: true
  target path: fs.path(:sample_logs, '%Y%m%d.apache.log')
  method_option :min_users, :type => :numeric, :desc => 'Min number of users in sample', :default => 10
  method_option :max_users, :type => :numeric, :desc => 'Max number of users in sample', :default => 20
  method_option :min_visits, :type => :numeric, :desc => 'Min number of visits per day in sample', :default => 10
  method_option :max_visits, :type => :numeric, :desc => 'Max number of visits per day in sample', :default => 100
  def generate_sample_logs_task
    targets.missing do |target|
      fs.write(generator.random_logs(target.start_date), target.path)
    end
  end

  private

  def generator
    @generator ||= RandomApacheLogGenerator.new(options.to_h.symbolize_keys)
  end
end
