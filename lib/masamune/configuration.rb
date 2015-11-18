#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'delegate'
require 'yaml'
require 'tilt/erb'
require 'pp'

require 'active_support/core_ext/hash'

require 'masamune/has_environment'

class Masamune::Configuration
  extend Forwardable
  include Masamune::HasEnvironment

  attr_accessor :quiet
  attr_accessor :verbose
  attr_accessor :debug
  attr_accessor :dry_run
  attr_accessor :lock
  attr_accessor :retries
  attr_accessor :backoff
  attr_accessor :params

  COMMANDS = %w(hive hadoop_streaming hadoop_filesystem elastic_mapreduce s3cmd postgres postgres_admin)
  COMMANDS.each do |command|
    attr_accessor command
    define_method(command) do
      instance_variable_get("@#{command}").symbolize_keys!
    end
  end

  def initialize(environment)
    self.environment   = environment
    self.quiet    = false
    self.verbose  = false
    self.debug    = false
    self.dry_run  = false
    self.lock     = nil
    self.retries  = 3
    self.backoff  = 5
    self.params   = HashWithIndifferentAccess.new

    @templates    = Hash.new { |h,k| h[k] = {} }

    COMMANDS.each do |command|
      instance_variable_set("@#{command}", {})
    end
  end

  def load(path)
    @load_once ||= begin
      config_file = filesystem.eval_path(path)
      load_yaml_erb_file(config_file).each_pair do |command, value|
        if COMMANDS.include?(command)
          send("#{command}=", value)
        elsif command == 'paths'
          load_paths(value)
        elsif command == 'params'
          raise ArgumentError, 'params section must only contain key value pairs' unless value.is_a?(Hash)
          self.params.merge! value
        end
      end
      logger.debug("Loaded configuration #{config_file}")
      load_catalog(configuration.postgres.fetch(:schema_files, []) + configuration.hive.fetch(:schema_files, []))
      true
    end
  end

  def load_catalog(paths = [])
    paths.each do |path|
      filesystem.glob_sort(path, order: :basename) do |file|
        configuration.with_quiet do
          catalog.load(file)
        end
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

  def debug=(debug)
    @debug = debug
    environment.reload_logger!
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
    opts << '--dry-run' if dry_run
    opts
  end

  def_delegators :filesystem, :add_path, :get_path

  def with_quiet(&block)
    prev_quiet, self.quiet = quiet, true
    yield
  ensure
    self.quiet = prev_quiet
  end

  def load_yaml_erb_file(file)
    t = ERB.new(File.read(file))
    t.filename = file
    YAML.load(t.result(binding))
  end

  class << self
    def default_config_file=(config_file)
      @default_config_file = config_file
    end

    def default_config_file
      @default_config_file ||= File.join(File.expand_path('../../../', __FILE__), 'config', 'masamune.yml.erb')
    end
  end

  def default_config_file
    self.class.default_config_file
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

  def load_paths(paths)
    paths.each do |value|
      symbol, path, options = *value.to_a.flatten
      add_path(symbol, path, options)
    end
  end
end
