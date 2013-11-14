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

  COMMANDS = %w(hive hadoop_streaming hadoop_filesystem elastic_mapreduce s3cmd postgres)
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
      attributes.symbolize_keys!
      send(command).tap do |instance|
        resolve_path(command, attributes[:path]) if attributes[:path]
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
    @templates    = Hash.new { |h,k| h[k] = {} }
  end

  def load(file)
    @load_once ||= begin
      load_yaml_erb_file(file).each_pair do |command, value|
        send("#{command}=", value) if COMMANDS.include?(command)
      end
      logger.debug("Loaded configuration #{file}")
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

  def elastic_mapreduce_enabled?
    elastic_mapreduce.fetch(:enabled, false)
  end

  def jobflow
    return unless elastic_mapreduce_enabled?
    @jobflow
  end

  def jobflow=(jobflow)
    return unless jobflow
    @jobflow = defined_jobflows.fetch(jobflow.to_sym, jobflow.to_s)
  end

  def bind_template(section, template, input_args = {})
    free_command = load_template(section, template)[:command].split(/\s+/)
    [].tap do |bind_command|
      free_command.each do |free_expr|
        if free_expr =~ /(%.*)/
          free_param = $1
          bind_command << free_expr.gsub!(free_param, bind_param(section, template, free_param, input_args))
        elsif param = free_expr
          bind_command << param
        end
      end
    end
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
        debug ? Masamune::MultiIO.new($stderr, log_file) : log_file
      else
        debug ? $stderr : nil
      end
      Logger.new(log_file_io)
    end
  end

  def print(*a)
    line = a.join(' ').chomp
    client.mutex.synchronize do
      logger.info(line)
      $stdout.puts line if !quiet && !debug
      $stdout.flush
    end
  end

  def trace(*a)
    line = a.join(' ').chomp
    client.mutex.synchronize do
      logger.info(line)
      $stdout.puts line if verbose && !debug
      $stdout.flush
    end
  end

  def filesystem
    @filesystem ||= begin
      filesystem = Masamune::Filesystem.new
      filesystem.add_path :root_dir, File.expand_path('../../../', __FILE__)
      filesystem = Masamune::MethodLogger.new(filesystem, :copy_file, :remove_dir, :move_file)
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
    {:path => 'hive', :database => 'default', :options => []}
  end

  def default_hadoop_streaming_attributes
    {:path => 'hadoop', :jar => default_hadoop_streaming_jar, :options => []}
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

  def default_hadoop_filesystem_attributes
    {:path => 'hadoop', :options => []}
  end

  def default_elastic_mapreduce_attributes
    {:path => 'elastic-mapreduce', :enabled => false, :options => []}
  end

  def default_s3cmd_attributes
    {:path => 's3cmd', :options => []}
  end

  def default_postgres_attributes
    {:path => 'psql', :database => 'postgres', :options => []}
  end

  def resolve_path(command, path)
    `which #{path}`.chomp.present? or raise ::Thor::InvocationError, "Invalid path #{path} for command #{command}"
  end

  def defined_jobflows
    @defined_jobflows ||= (elastic_mapreduce.fetch(:jobflows, {}) || {}).symbolize_keys
  end

  private

  def load_template(section, template_name)
    @templates[section][template_name] ||= begin
      raise ArgumentError, "no configuration section #{section}" unless COMMANDS.include?(section.to_s)
      raise ArgumentError, 'no template_name' unless template_name
      templates = send(section).fetch(:templates, {}).symbolize_keys!
      template = templates[template_name.to_sym] or raise ArgumentError, "no template for #{template_name}"
      template.symbolize_keys!
      template.has_key?(:command) or raise ArgumentError, "no command for template #{template_name}"
      template[:default] ||= {}
      template[:default] = Hash[ template[:default].collect { |key,val| [key.to_sym, val.to_s] } ]
      template
    end
  end

  def bind_param(section, template, free_param, input_args = {})
    default = load_template(section, template).fetch(:default, {})
    param = free_param[/(?<=%).*/].to_sym
    default.merge(input_args.symbolize_keys || {})[param] or raise ArgumentError, "no param for #{free_param}"
  end
end
