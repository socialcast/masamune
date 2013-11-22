require 'delegate'
require 'thread'
require 'tmpdir'
require 'logger'

require 'masamune/version'
require 'masamune/multi_io'

module Masamune
  module ContextBehavior
    extend Forwardable

    def context
      @context || Masamune.default_context
    end

    def context=(context)
      @context = context
    end

    def_delegators :context, :configure, :configuration, :with_exclusive_lock, :logger, :filesystem, :filesystem=, :trace, :print
  end

  class Context
    attr_accessor :parent
    attr_accessor :filesystem

    def initialize(parent = nil)
      self.parent = parent
    end

    def version
      "masamune #{Masamune::VERSION}"
    end

    def configure
      yield configuration
    end

    def configuration
      @configuration ||= Masamune::Configuration.new(self)
    end

    def mutex
      @mutex ||= Mutex.new
    end

    def with_exclusive_lock(name, &block)
      logger.debug("acquiring lock '#{name}'")
      lock_file = lock_file(name)
      lock_status = lock_file.flock(File::LOCK_EX | File::LOCK_NB)
      if lock_status == 0
        yield
      else
        raise "acquire lock attempt failed for '#{name}'"
      end
    ensure
      logger.debug("releasing lock '#{name}'")
      lock_file.flock(File::LOCK_UN)
    end

    def log_file_template
      @log_file_template || "#{Time.now.to_i}-#{$$}.log"
    end

    def log_file_template=(log_file_template)
      @log_file_template = log_file_template
      reload_logger!
    end

    def reload_logger!
      @logger = nil
    end

    def log_enabled?
      if parent && parent.respond_to?(:log_enabled?)
        parent.log_enabled?
      else
        true
      end
    end

    def logger
      @logger ||= begin
        log_file_io = if log_enabled? && filesystem.has_path?(:log_dir)
          log_file = File.open(File.join(filesystem.path(:log_dir), log_file_template), 'a')
          log_file.sync = true
          FileUtils.ln_s(log_file, File.join(filesystem.path(:log_dir), 'latest'), force: true)
          configuration.debug ? Masamune::MultiIO.new($stderr, log_file) : log_file
        else
          configuration.debug ? $stderr : nil
        end
        Logger.new(log_file_io)
      end
    end

    def print(*a)
      line = a.join(' ').chomp
      mutex.synchronize do
        logger.info(line)
        $stdout.puts line if !configuration.quiet && !configuration.debug
        $stdout.flush
      end
    end

    def trace(*a)
      line = a.join(' ').chomp
      mutex.synchronize do
        logger.info(line)
        $stdout.puts line if configuration.verbose && !configuration.debug
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

    private

    def lock_file(name)
      path =
      if configuration.filesystem.has_path?(:var_dir)
        configuration.filesystem.get_path(:var_dir, "#{name}.lock")
      else
        File.join(Dir.tmpdir, "#{name}.lock")
      end
      File.open(path, File::CREAT, 0644)
    end
  end
end
