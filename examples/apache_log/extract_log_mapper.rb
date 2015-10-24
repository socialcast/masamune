#!/usr/bin/env ruby

APACHE_LOG_REGEX = /^(?<ip_address>\d+\.\d+\.\d+\.\d+) - (?<user_id>\d+) \[(?<timestamp>.*?)\] "GET (?<path>.*?) HTTP\/1.1" \d+ \d+ "-" "(?<user_agent>.*?)"/

ARGF.each do |line|
  next unless fields = APACHE_LOG_REGEX.match(line)
  next unless fields[:timestamp] && fields[:ip_address] && fields[:path]
  created_at = DateTime.strptime(fields[:timestamp], '%d/%b/%Y:%H:%M:%S %z').to_time.utc.iso8601
  puts [created_at, fields[:user_id].to_i, fields[:ip_address], fields[:path], fields[:user_agent]].join("\t")
end
