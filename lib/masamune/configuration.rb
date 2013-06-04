require 'logger'
require 'masamune/multi_io'

class Masamune::Configuration
  extend Forwardable

  attr_accessor :debug
  attr_accessor :log_dir
  attr_accessor :log_file_template
  attr_accessor :var_dir
  attr_accessor :logger
  attr_accessor :filesystem
  attr_accessor :command_options
  attr_accessor :dryrun
  attr_accessor :elastic_mapreduce
  attr_accessor :jobflow
  attr_accessor :hadoop_streaming_jar
  attr_accessor :hive_database

  def dryrun
    @dryrun ||= false
  end

  def debug
    @debug ||= false
  end

  def elastic_mapreduce
    @elastic_mapreduce ||= false
  end

  def hive_database
    @hive_database ||= 'default'
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
    @log_file_template ||= "masamune-#{$$}.log"
  end

  def logger
    @logger ||= begin
      log_file = File.open(File.join(log_dir, log_file_template), 'a')
      log_file.sync = true
      debug ? Logger.new(Masamune::MultiIO.new(STDERR, log_file)) : Logger.new(log_file)
    end
  end

  def print(*a)
    logger.info(*a)
    puts a.join(' ') unless debug
  end

  def trace(*a)
    logger.info(*a)
    puts a.join(' ') unless debug
  end

  def filesystem
    @filesystem ||=
      Masamune::CachedFilesystem.new(
        Masamune::MethodLogger.new(
          Masamune::Filesystem.new, :ignore => [:path, :exists?, :glob, :get_path, :add_path]))
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

  def command_options
    @command_options ||= {}.tap do |h|
      h.default = Proc.new { [] }
    end
  end

  def add_command_options(command, &block)
    command_options[command] = block.to_proc
  end
end
