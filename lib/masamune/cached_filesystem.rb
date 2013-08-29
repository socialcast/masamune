require 'delegate'

module Masamune
  class CachedFilesystem < Delegator
    include Masamune::Accumulate

    def initialize(filesystem)
      super
      @filesystem = filesystem
      clear!
    end

    def clear!
      @path_cache = Set.new
    end

    def exists?(file)
      glob(file).any?
    end

    def glob(wildcard, &block)
      pattern = /\A#{wildcard.gsub('*', '.*?')}\Z/
      dirname = File.dirname(wildcard)

      if @path_cache.include?(dirname)
        @path_cache.each do |file|
          yield file if file =~ pattern
        end
      else
        @path_cache.merge(glob_with_sub_paths(wildcard))

        @path_cache.each do |file|
          yield file if file =~ pattern
        end
      end
    end
    method_accumulate :glob

    # FIXME cache eviction policy can be more precise
    [:touch!, :mkdir!, :copy_file, :remove_dir, :move_file, :write].each do |meth|
      define_method(meth) do |*args|
        clear!
        @filesystem.send(meth, *args)
      end
    end

    def sub_paths(file, &block)
      path = []
      file.split('/').each do |part|
        path << part
        full = path.join('/')
        yield full.blank? ? '/' : full
      end
    end

    def glob_with_sub_paths(wildcard)
      dirname = File.dirname(wildcard)
      Set.new.tap do |paths|
        @filesystem.glob(File.join(dirname, '*')) do |file|
          sub_paths(file) { |path| paths.add path }
        end

        if paths.empty? && @filesystem.exists?(dirname)
          sub_paths(dirname) { |path| paths.add path }
        end
      end
    end

    def __getobj__
      @filesystem
    end

    def __setobj__(obj)
      @filesystem = obj
    end
  end
end
