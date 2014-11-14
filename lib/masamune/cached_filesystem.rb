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
      @cache = Hash.new { |h,k| h[k] = Set.new }
    end

    def exists?(file)
      @cache.key?(file) || glob(file).include?(file) || @cache.key?(file)
    end

    def glob(file_or_glob, &block)
      glob_stat(file_or_glob) do |entry|
        yield entry.name
      end
    end
    method_accumulate :glob

    # FIXME cache eviction policy can be more precise
    [:touch!, :mkdir!, :copy_file_to_file, :copy_file_to_dir, :copy_dir, :remove_file, :remove_dir, :move_file_to_file, :move_file_to_dir, :move_dir, :write, :glob_sort].each do |meth|
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

    MAX_DEPTH   = 10
    CACHE_DEPTH = 1
    EMPTY_SET   = Set.new

    def glob_stat(file_or_glob, depth: 0, &block)
      return if file_or_glob.blank?
      return if depth > MAX_DEPTH || depth > CACHE_DEPTH

      glob_stat(File.join(@filesystem.dirname(file_or_glob)), depth: depth + 1, &block)

      dirname = @filesystem.dirname(file_or_glob)
      unless @cache.key?(dirname)
        @filesystem.glob_stat(File.join(dirname, '*')) do |entry|
          recursive_paths(dirname, entry.name) do |path|
            @cache[path] << entry
          end
        end
      end
      @cache[dirname] ||= EMPTY_SET

      file_regexp = glob_to_regexp(file_or_glob, recursive: false)
      @cache[dirname].each do |entry|
        yield entry if entry.name =~ file_regexp
      end if depth == 0
    end

    def recursive_paths(root, path, depth: 0, &block)
      return if depth > MAX_DEPTH
      return if root == path
      yield @filesystem.dirname(path)
      recursive_paths(root, @filesystem.dirname(path), depth: depth + 1, &block)
    end
  end
end
