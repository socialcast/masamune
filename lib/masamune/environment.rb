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

require 'thread'
require 'logger'

require 'masamune/version'
require 'masamune/multi_io'

module Masamune
  class Environment
    attr_accessor :parent
    attr_accessor :filesystem
    attr_accessor :catalog

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
      @configuration ||= Masamune::Configuration.new(environment: self)
    end

    def mutex
      @mutex ||= Mutex.new
    end

    def with_exclusive_lock(name)
      raise 'filesystem path :run_dir not defined' unless filesystem.path?(:run_dir)
      lock_name = [name, configuration.lock].compact.join(':')
      logger.debug("acquiring lock '#{lock_name}'")
      lock_file = lock_file(lock_name)
      lock_mode = File::LOCK_EX
      lock_mode |= File::LOCK_NB
      lock_status = lock_file.flock(lock_mode)
      if lock_status && lock_status.zero?
        yield if block_given?
      else
        logger.error "acquire lock attempt failed for '#{lock_name}'"
      end
    ensure
      if lock_file && lock_status && lock_status.zero?
        logger.debug("releasing lock '#{lock_name}'")
        lock_file.flock(File::LOCK_UN)
      end
    end

    def with_process_lock(name)
      with_exclusive_lock("#{name}_#{Process.pid}") do
        yield
      end
    end

    def log_file_template
      @log_file_template || "#{Time.now.to_i}-#{$PROCESS_ID}.log"
    end

    def log_file_template=(log_file_template)
      @log_file_template = log_file_template
      reload_logger!
    end

    def reload_logger!
      @logger = @log_file_name = nil
    end

    def log_file_name
      return unless filesystem.path?(:log_dir)
      @log_file_name ||= filesystem.get_path(:log_dir, log_file_template)
    end

    def logger
      @logger ||= Logger.new(log_file_io)
    end

    def console(*a)
      line = a.join(' ').chomp
      mutex.synchronize do
        logger.info(line)
        $stdout.puts line unless configuration.quiet || configuration.debug
        $stdout.flush
        $stderr.flush
      end
    end

    def trace(*a)
      line = a.join(' ').chomp
      mutex.synchronize do
        logger.info(line)
        $stdout.puts line if configuration.verbose && !configuration.debug
        $stdout.flush
        $stderr.flush
      end
    end

    def filesystem
      @filesystem ||= begin
        filesystem = Masamune::Filesystem.new
        filesystem.add_path :root_dir, File.expand_path('../../../', __FILE__)
        filesystem = Masamune::MethodLogger.new(filesystem, :copy_file_to_file, :copy_file_to_dir, :remove_dir, :move_file_to_file, :move_file_to_dir, :move_dir)
        filesystem
      end
    end

    def catalog
      @catalog ||= Masamune::Schema::Catalog.new(self)
    end

    def hive_helper
      @hive_helper ||= Masamune::Helpers::Hive.new(self)
    end

    def postgres_helper
      @postgres_helper ||= Masamune::Helpers::Postgres.new(self)
    end

    private

    def lock_file(name)
      path = filesystem.get_path(:run_dir, "#{name}.lock")
      File.open(path, File::CREAT, 0644)
    end

    def log_file_io
      if filesystem.path?(:log_dir)
        log_file = File.open(log_file_name, 'a')
        log_file.sync = true
        configuration.debug ? Masamune::MultiIO.new($stderr, log_file) : log_file
      else
        configuration.debug ? $stderr : nil
      end
    end
  end
end
