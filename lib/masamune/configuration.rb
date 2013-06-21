require 'logger'
require 'forwardable'
require 'yaml'
require 'erb'
require 'pp'

require 'masamune/version'
require 'masamune/multi_io'

class Masamune::Configuration
  extend Forwardable

  attr_accessor :client
  attr_accessor :quiet
  attr_accessor :verbose
  attr_accessor :debug
  attr_accessor :no_op
  attr_accessor :dry_run
  attr_accessor :jobflow
  attr_accessor :context

  attr_accessor :logger
  attr_accessor :filesystem

  attr_accessor :log_file_template

  COMMANDS = %w(hive hadoop_streaming hadoop_filesystem elastic_mapreduce s3cmd)
  COMMANDS.each do |command|
    define_method(command) do
      unless instance_variable_get("@#{command}")
        if respond_to?(:"default_#{command}_attributes")
          instance_variable_set("@#{command}", send(:"default_#{command}_attributes"))
        else
          instance_variable_set("@#{command}", send(:default_command_attributes))
        end
      end
      instance_variable_get("@#{command}").symbolize_keys!
      instance_variable_get("@#{command}")
    end

    define_method("#{command}=") do |attributes|
      send(command).tap do |instance|
        instance[:options] ||= []
        if options = attributes.delete(:options)
          instance[:options] += options
        end
        instance.merge!(attributes)
      end
    end
  end

  def initialize(client)
    self.client   = client
    self.quiet    = false
    self.verbose  = false
    self.debug    = false
    self.no_op    = false
    self.dry_run  = false
  end

  def load(file)
    @load_once ||= begin
      load_yaml_erb_file(file).each_pair do |command, value|
        send("#{command}=", value) if COMMANDS.include?(command)
      end
    end
  end

  def to_s
    io = StringIO.new
    rep = {"path" => filesystem.paths}
    COMMANDS.each do |command|
      rep[command] = send(command)
    end
    PP.pp(rep, io)
    io.string
  end

  def version
    "masamune #{Masamune::VERSION}"
  end

  def debug=(debug)
    @debug = debug
    @logger = nil
  end

  def log_enabled?
    if self.client.context && client.context.respond_to?(:log_enabled?)
      self.client.context.log_enabled?
    else
      true
    end
  end

  def log_file_template
    @log_file_template || "#{Time.now.to_i}-#{$$}.log"
  end

  def log_file_template=(log_file_template)
    @log_file_template = log_file_template
    @logger = nil
  end

  def logger
    @logger ||= begin
      log_file_io = if log_enabled? && filesystem.has_path?(:log_dir)
        log_file = File.open(File.join(filesystem.path(:log_dir), log_file_template), 'a')
        log_file.sync = true
        FileUtils.ln_s(log_file, File.join(filesystem.path(:log_dir), 'latest'), force: true)
        debug ? Masamune::MultiIO.new(STDERR, log_file) : log_file
      else
        debug ? STDERR : nil
      end
      Logger.new(log_file_io)
    end
  end

  def print(*a)
    client.mutex.synchronize do
      logger.info(*a)
      puts a.join(' ') if !quiet && !debug
    end
  end

  def trace(*a)
    client.mutex.synchronize do
      logger.info(*a)
      puts a.join(' ') if verbose && !debug
    end
  end

  def filesystem
    @filesystem ||= begin
      filesystem = Masamune::Filesystem.new
      filesystem.add_path :root_dir, File.expand_path('../../../', __FILE__)
      filesystem = Masamune::MethodLogger.new(filesystem, :ignore => [:path, :paths, :get_path, :add_path, :has_path?, :exists?, :glob])
      Masamune::CachedFilesystem.new(filesystem)
    end
  end

  def as_options
    opts = []
    opts << '--quiet'   if quiet
    opts << '--verbose' if verbose
    opts << '--debug'   if debug
    opts << '--no_op'   if no_op
    opts << '--dry_run' if dry_run
    opts << "--jobflow=#{jobflow}" if jobflow
    opts
  end

  def_delegators :filesystem, :add_path, :get_path

  def load_yaml_erb_file(file)
    YAML.load(ERB.new(File.read(file)).result(binding))
  end

  def default_command_attributes
    {:options => []}
  end

  def default_hive_attributes
    {:database => 'default', :options => []}
  end

  def default_hadoop_streaming_attributes
    {:jar => default_hadoop_streaming_jar, :options => []}
  end

  def default_hadoop_streaming_jar
    case RUBY_PLATFORM
    when /darwin/
      '/usr/local/Cellar/hadoop/1.1.2/libexec/contrib/streaming/hadoop-streaming-1.1.2.jar'
    when /linux/
      '/usr/lib/hadoop-mapreduce/hadoop-streaming.jar'
    else
      raise 'hadoop_streaming_jar not found'
    end
  end

  def default_elastic_mapreduce_attributes
    {:enabled => false, :options => []}
  end
end
