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
      dirname = @filesystem.dirname(wildcard)

      unless @path_cache.include?(dirname)
        @path_cache.merge(glob_with_parent_paths(wildcard))
      end

      @path_cache.each do |file|
        yield file if file =~ pattern
      end
    end
    method_accumulate :glob

    # FIXME cache eviction policy can be more precise
    [:touch!, :mkdir!, :copy_file, :remove_dir, :move_file, :move_dir, :write, :glob_sort].each do |meth|
      define_method(meth) do |*args|
        clear!
        @filesystem.send(meth, *args)
      end
    end

    def __getobj__
      @filesystem
    end

    def __setobj__(obj)
      @filesystem = obj
    end

    private

    def glob_with_parent_paths(wildcard)
      dirname = @filesystem.dirname(wildcard)
      Set.new.tap do |paths|
        @filesystem.glob(File.join(dirname, '*')) do |file|
          @filesystem.parent_paths(file) { |path| paths.add path }
          paths.add file
        end

        if paths.empty? && @filesystem.exists?(dirname)
          @filesystem.parent_paths(dirname) { |path| paths.add path }
          paths.add dirname
        end
      end
    end
  end
end
