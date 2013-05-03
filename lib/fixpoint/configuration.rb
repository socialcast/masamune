require 'logger'
require 'fixpoint/multi_io'

class Fixpoint::Configuration
  attr_accessor :debug
  attr_accessor :log_dir
  attr_accessor :log_file_template
  attr_accessor :logger

  def debug
    @debug ||= false
  end

  def log_dir
    @log_dir ||= File.expand_path('../../../log/', __FILE__)
  end

  def log_file_template
    @log_file_template ||= "fixpoint-#{$$}.log"
  end

  def logger
    @logger ||= begin
      FileUtils.mkdir_p(log_dir) unless File.exists?(log_dir)
      log_file = File.open(File.join(log_dir, log_file_template), 'a')
      log_file.sync = true
      debug ? Logger.new(Fixpoint::MultiIO.new(STDERR, log_file)) : Logger.new(log_file)
    end
  end
end
