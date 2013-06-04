require 'logger'
require 'masamune/multi_io'

class Masamune::Configuration
  extend Forwardable

  attr_accessor :quiet
  attr_accessor :verbose
  attr_accessor :debug
  attr_accessor :no_op
  attr_accessor :dry_run
  attr_accessor :jobflow

  attr_accessor :log_file_template
  attr_accessor :logger
  attr_accessor :filesystem
  attr_accessor :command_options
  attr_accessor :elastic_mapreduce
  attr_accessor :hadoop_streaming_jar
  attr_accessor :hive_database

  def initialize
    self.quiet    = false
    self.verbose  = false
    self.debug    = false
    self.no_op    = false
    self.dry_run  = false
  end

  def elastic_mapreduce
    @elastic_mapreduce ||= false
  end

  def hive_database
    @hive_database ||= 'default'
  end

  def logger
    @logger ||= begin
      # TODO symlink latest
      log_file = File.open(File.join(filesystem.path(:log_dir), log_file_template), 'a')
      log_file.sync = true
      debug ? Logger.new(Masamune::MultiIO.new(STDERR, log_file)) : Logger.new(log_file)
    end
  end

  def print(*a)
    logger.info(*a)
    puts a.join(' ') if !quiet && !debug
  end

  def trace(*a)
    logger.info(*a)
    puts a.join(' ') if verbose && !debug
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
