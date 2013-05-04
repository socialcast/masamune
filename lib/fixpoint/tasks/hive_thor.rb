require 'fixpoint'

class HiveThor < Thor
  include Fixpoint::Actions::Hive

  namespace :fixpoint
  class_option :debug, :aliases => '-d', :desc => 'Print debugging information', :default => false

  def initialize(*a)
    super
    Fixpoint.configure do |config|
      config.debug = options[:debug]
    end
  end

  desc 'hive', 'hive'
  method_option :file, :aliases => '-f', :desc => 'SQL from files'
  method_option :exec, :aliases => '-e', :desc => 'SQL from command line'
  method_option :jobflow, :aliases => '-j', :desc => 'EMR jobflow ID'
  def hive_exec
    hive(options)
  end
end
