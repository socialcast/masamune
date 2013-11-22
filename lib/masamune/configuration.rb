require 'delegate'
require 'yaml'
require 'erb'
require 'pp'

require 'active_support/core_ext/hash'

class Masamune::Configuration
  extend Forwardable
  include Masamune::ClientBehavior

  attr_accessor :quiet
  attr_accessor :verbose
  attr_accessor :debug
  attr_accessor :no_op
  attr_accessor :dry_run

  COMMANDS = %w(hive hadoop_streaming hadoop_filesystem elastic_mapreduce s3cmd postgres postgres_admin)
  COMMANDS.each do |command|
    attr_accessor command
    define_method(command) do
      instance_variable_get("@#{command}").symbolize_keys!
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

    COMMANDS.each do |command|
      instance_variable_set("@#{command}", {})
    end
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

  def debug=(debug)
    @debug = debug
    client.reload_logger!
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

  private

  def load_template(section, template_name)
    @templates[section][template_name] ||= begin
      raise ArgumentError, "no configuration section #{section}" unless COMMANDS.include?(section.to_s)
      raise ArgumentError, 'no template_name' unless template_name
      templates = send(section).fetch(:templates, {})
      template = templates[template_name.to_sym] or raise ArgumentError, "no template for #{template_name}"
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
