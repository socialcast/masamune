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

require 'net/http'

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
      fs.write(random_logs(target.start_date), target.path)
    end
  end

  private

  def random_logs(date)
    logs = []
    rand(options[:min_visits] .. options[:max_visits]).times do
      logs << random_log(date) 
    end
    logs.join("\n")
  end

  def random_log(date)
    user = users.sample
    '%s - %d [%s] "GET %s HTTP/1.1" 200 %d "-" "%s"' % [user.ip, user.id, random_date(date), random_path, random_size, user.ua]
  end

  def random_date(date)
    (date + rand(24).hours + rand(60).minutes + rand(60).seconds).strftime('%d/%b/%Y:%H:%M:%S %z')
  end

  def random_path
    paths.sample
  end

  def random_ip_address
    Array.new(4) { rand(255) }.join('.')
  end

  def random_user_agent
    JSON.parse(Net::HTTP.get(user_agent_io_uri))['ua']
  rescue
    fallback_user_agents.sample
  end

  def random_size
    rand(1 << 15)
  end

  def users
    @users ||= begin
      n = rand(options[:min_users] .. options[:max_users])
      console("Generating #{n} random users")
      Parallel.map(1 .. n) do
        OpenStruct.new(id: rand(1 << 15), ua: random_user_agent, ip: random_ip_address)
      end
    end
  end

  def paths
    @paths ||= %w(/home /streams /profile /groups) + users.map { |user| "/users/#{user.id}" }
  end

  def user_agent_io_uri
    @user_agent_io_uri ||= URI.parse('http://api.useragent.io/')
  end

  def fallback_user_agents
    @fallback_user_agents ||= [
      'Mozilla/5.0 (X11; U; Linux i686; es-ES; rv:1.8.1.2) Gecko/20060601 Firefox/2.0.0.2 (Ubuntu-edgy)',
      'Mozilla/5.0 (Windows; U; Windows NT 5.1; es-AR; rv:1.9.1.19) Gecko/20110420 SeaMonkey/2.0.14',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_7) AppleWebKit/534.24 (KHTML, like Gecko) RockMelt/0.9.58.423 Chrome/11.0.696.71 Safari/534.24',
      'Opera/9.80 (Windows NT 6.1; U; en-US) Presto/2.7.62 Version/11.01',
      'Mozilla/5.0 (X11; U; NetBSD amd64; fr-FR; rv:1.8.0.7) Gecko/20061102 Firefox/1.5.0.7'
    ]
  end
end
