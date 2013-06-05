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
      log_file_io = if filesystem.has_path?(:log_dir)
        log_file = File.open(File.join(filesystem.path(:log_dir), log_file_template), 'a')
        log_file.sync = true
        FileUtils.ln_s(log_file, File.join(filesystem.path(:log_dir), 'latest'), force: true)
        debug ? Masamune::MultiIO.new(STDERR, log_file) : log_file
      end
      Logger.new(log_file_io)
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
          Masamune::Filesystem.new, :ignore => [:path, :get_path, :add_path, :has_path?, :exists?, :glob]))
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

  def as_options
    opts = []
    opts << '--quiet'   if verbose
    opts << '--verbose' if verbose
    opts << '--debug'   if debug
    opts << '--no_op'   if no_op
    opts << '--dry_run' if dry_run
    opts << "--jobflow=#{jobflow}" if jobflow
    opts
  end
end
