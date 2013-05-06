require 'logger'
require 'fixpoint/multi_io'

class Fixpoint::Configuration
  attr_accessor :debug
  attr_accessor :log_dir
  attr_accessor :log_file_template
  attr_accessor :var_dir
  attr_accessor :logger
  attr_accessor :filesystem

  def debug
    @debug ||= false
  end

  def log_dir
    @log_dir ||= File.expand_path('../../../log/', __FILE__).tap do |log_dir|
      FileUtils.mkdir_p(log_dir) unless File.exists?(log_dir)
    end
  end

  def var_dir
    @var_dir ||= File.expand_path('../../../var/', __FILE__).tap do |var_dir|
      FileUtils.mkdir_p(var_dir) unless File.exists?(var_dir)
    end
  end

  def log_file_template
    @log_file_template ||= "fixpoint-#{$$}.log"
  end

  def logger
    @logger ||= begin
      log_file = File.open(File.join(log_dir, log_file_template), 'a')
      log_file.sync = true
      debug ? Logger.new(Fixpoint::MultiIO.new(STDERR, log_file)) : Logger.new(log_file)
    end
  end

  def filesystem
    @filesystem ||= Fixpoint::Filesystem::Hadoop.new
  end

  def hadoop_streaming_jar
    @hadoop_streaming_jar ||= begin
      case RUBY_PLATFORM
      when /darwin/
        '/usr/local/Cellar/hadoop/1.1.2/libexec/contrib/streaming/hadoop-streaming-1.1.2.jar'
      when /linux/
        '/usr/lib/hadoop-mapreduce/hadoop-streaming.jar'
      else
        raise 'hadoop_streaming_jar not found'
      end
    end
  end
end
