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
        yield entry.name unless entry.name == dirname(file_or_glob)
      end
    end
    method_accumulate :glob

    def stat(file_or_dir)
      raise ArgumentError, 'cannot contain wildcard' if file_or_dir.include?('*')
      result = Set.new
      glob_stat(file_or_dir) do |entry|
        result << entry
      end
      result += @cache[file_or_dir]
      return unless result.any?
      return result.first if result.size == 1
      max_time = result.map { |stat| stat.try(:mtime) }.compact.max
      sum_size = result.map { |stat| stat.try(:size) }.compact.reduce(:+)
      OpenStruct.new(name: file_or_dir, mtime: max_time, size: sum_size)
    end

    # FIXME cache eviction policy can be more precise
    [:touch!, :mkdir!, :copy_file_to_file, :copy_file_to_dir, :copy_dir, :remove_file, :remove_dir, :move_file_to_file, :move_file_to_dir, :move_dir, :write].each do |method|
      define_method(method) do |*args|
        clear!
        @filesystem.send(method, *args)
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
      return if root_path?(file_or_glob)
      return if depth > MAX_DEPTH || depth > CACHE_DEPTH

      glob_stat(dirname(file_or_glob), depth: depth + 1, &block)

      dirname = dirname(file_or_glob)
      unless @cache.key?(dirname)
        pattern = root_path?(dirname) ? file_or_glob : File.join(dirname, '*')
        @filesystem.glob_stat(pattern) do |entry|
          recursive_paths(dirname, entry.name) do |path|
            @cache[path] << entry
          end
        end
      end
      @cache[dirname] ||= EMPTY_SET

      file_regexp = glob_to_regexp(file_or_glob)
      @cache[dirname].each do |entry|
        yield entry if entry.name =~ file_regexp
      end if depth == 0
    end

    def recursive_paths(root, path, depth: 0, &block)
      return if depth > MAX_DEPTH
      return if root == path
      yield path
      yield dirname(path)
      recursive_paths(root, dirname(path), depth: depth + 1, &block)
    end
  end
end
