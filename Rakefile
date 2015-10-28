#!/usr/bin/env rake

begin
  require 'bundler/setup'
rescue LoadError
  puts 'You must `gem install bundler` and `bundle install` to run rake tasks'
end

Bundler::GemHelper.install_tasks

require 'rspec/core/rake_task'

desc 'Run Rspec unit code examples'
RSpec::Core::RakeTask.new(:spec)

namespace :spec do
  desc 'Run Rspec unit code examples'
  RSpec::Core::RakeTask.new(:unit) do |spec|
    spec.pattern = "spec/**/*_spec.rb"
  end

  desc 'Run Rspec acceptance code examples'
  RSpec::Core::RakeTask.new(:acceptance) do |spec|
    spec.pattern = "examples/**/*_spec.rb"
  end

  desc 'Run All Rspec code examples'
  task all: [:unit, :acceptance]
end

task :default => :spec
