#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
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
require 'hashie'

require 'masamune/has_environment'

class Masamune::Configuration < Hashie::Dash
  extend Forwardable

  include Hashie::Extensions::MergeInitializer
  include Hashie::Extensions::IndifferentAccess

  class << self
    attr_writer :default_config_file

    def default_config_file
      @default_config_file ||= File.join(File.expand_path('../../../', __FILE__), 'config', 'masamune.yml.erb')
    end

    def default_commands
      @default_commands ||= %i(aws_emr hive hadoop_streaming hadoop_filesystem s3cmd postgres postgres_admin)
    end
  end

  property :environment
  include Masamune::HasEnvironment

  property :quiet, default: false
  property :verbose, default: false
  property :debug, default: false
  property :dry_run, default: false
  property :lock
  property :retries, default: 3
  property :backoff, default: 5
  property :params, default: Hashie::Mash.new
  property :commands, default: Hashie::Mash.new { |h, k| h[k] = Hashie::Mash.new }

  def initialize(*a)
    super
    self.class.default_commands.each do |command|
      commands[command] = Hashie::Mash.new
    end
  end

  def load(path)
    @load_once ||= begin
      config_file = filesystem.eval_path(path)
      load_yaml_erb_file(config_file).each_pair do |command, value|
        if command == 'commands'
          commands.merge!(value)
        elsif command == 'paths'
          load_paths(value)
        elsif command == 'params'
          raise ArgumentError, 'params section must only contain key value pairs' unless value.is_a?(Hash)
          params.merge! value
        end
      end
      logger.debug("Loaded configuration #{config_file}")
      load_catalog(configuration.commands.postgres.fetch(:schema_files, []) + configuration.commands.hive.fetch(:schema_files, []))
      self
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

  def debug=(debug)
    self[:debug] = debug
    environment.reload_logger!
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

  def with_quiet
    prev_quiet = quiet
    self.quiet = true
    yield
  ensure
    self.quiet = prev_quiet
  end

  def load_yaml_erb_file(file)
    t = ERB.new(File.read(file))
    t.filename = file
    YAML.load(t.result(binding))
  end

  def default_config_file
    self.class.default_config_file
  end

  private

  def load_paths(paths)
    paths.each do |value|
      symbol, path, options = *value.to_a.flatten
      add_path(symbol, path, options)
    end
  end
end
