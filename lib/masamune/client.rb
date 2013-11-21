require 'delegate'
require 'thread'
require 'tmpdir'

module Masamune
  module ClientBehavior
    extend Forwardable

    def client
      @client || Masamune.default_client
    end

    def client=(client)
      @client = client
    end

    def_delegators :client, :configure, :configuration, :with_exclusive_lock, :logger, :filesystem, :trace, :print
  end

  class Client
    extend Forwardable

    attr_accessor :context

    def initialize(context = nil)
      self.context = context
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
      Masamune.logger.debug("acquiring lock '#{name}'")
      lock_file = lock_file(name)
      lock_status = lock_file.flock(File::LOCK_EX | File::LOCK_NB)
      if lock_status == 0
        yield
      else
        raise "acquire lock attempt failed for '#{name}'"
      end
    ensure
      Masamune.logger.debug("releasing lock '#{name}'")
      lock_file.flock(File::LOCK_UN)
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

    def_delegators :configuration, :logger, :filesystem, :trace, :print
  end
end
