require 'delegate'

module Masamune
  class CachedFilesystem < Delegator
    def initialize(filesystem)
      super
      @filesystem = filesystem
      clear!
    end

    def clear!
      @path_cache = Set.new
      @glob_cache = Hash.new
    end

    def exists?(file)
      dirname = dirname(file)
      if @path_cache.include?(dirname)
        if @path_cache.include?(file)
          true
        else
          false
        end
      else
        glob(File.join(dirname, '*')) do |path|
          @path_cache = @path_cache.union(sub_paths(path))
        end
        @path_cache.include?(file)
      end
    end

    def glob(path, &block)
      @glob_cache[path] ||= begin
        if block_given?
          paths = Set.new
          @filesystem.glob(path) do |path|
            block.call(path)
            paths.add(path)
          end
          paths.to_a
        else
          @filesystem.glob(path)
        end
      end
    end

    # FIXME cache eviction policy can be more precise
    [:touch!, :mkdir!, :copy_file, :remove_dir, :move_file, :write].each do |meth|
      define_method(meth) do |*args|
        clear!
        @filesystem.send(meth, *args)
      end
    end

    def dirname(file)
      path = file.split('/')
      dirname, basename = path[0 .. -2].join('/'), path[-1]
      dirname
    end

    def sub_paths(file)
      [].tap do |result|
        tmp = []
        file.split('/').each do |part|
          tmp << part
          result << tmp.join('/')
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
