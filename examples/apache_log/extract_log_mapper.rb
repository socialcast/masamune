#!/usr/bin/env ruby
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

require 'user_agent_parser'

APACHE_LOG_REGEX = /^(?<ip_address>\d+\.\d+\.\d+\.\d+) - (?<user_id>\d+) \[(?<timestamp>.*?)\] "GET (?<path>.*?) HTTP\/1.1" \d+ \d+ "-" "(?<user_agent>.*?)"/

user_agent_parser = UserAgentParser::Parser.new

ARGF.each do |line|
  next unless fields = APACHE_LOG_REGEX.match(line)
  next unless fields[:timestamp] && fields[:ip_address] && fields[:path]
  created_at = DateTime.strptime(fields[:timestamp], '%d/%b/%Y:%H:%M:%S %z').to_time.utc
  user_agent = user_agent_parser.parse(fields[:user_agent])
  puts [created_at.strftime('%Y%m%d'), fields[:user_id].to_i, user_agent.family, user_agent.os.name, user_agent.device, created_at.to_i, 1].join("\t")
end
