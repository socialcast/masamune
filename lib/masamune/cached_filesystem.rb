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

module Masamune
  class CachedFilesystem < SimpleDelegator
    MAX_DEPTH   = 10
    EMPTY_SET   = Set.new

    def initialize(filesystem)
      super filesystem
      @filesystem = filesystem
      clear!
    end

    def clear!
      @cache = PathCache.new(@filesystem)
    end

    def exists?(file)
      glob(file, max_depth: 0).include?(file)
    end

    def glob(file_or_glob, options = {})
      return Set.new(to_enum(:glob, file_or_glob, options)) unless block_given?
      glob_stat(file_or_glob, options) do |entry|
        yield entry.name
      end
    end

    def stat(file_or_dir)
      raise ArgumentError, 'cannot contain wildcard' if file_or_dir.include?('*')
      result = glob_stat(file_or_dir, recursive: true)
      return unless result.any?
      max_time = result.map { |stat| stat.try(:mtime) }.compact.max
      sum_size = result.map { |stat| stat.try(:size) }.compact.reduce(:+)
      OpenStruct.new(name: file_or_dir, mtime: max_time, size: sum_size)
    end

    def glob_stat(file_or_glob, options = {}, &block)
      return Set.new(to_enum(:glob_stat, file_or_glob, options)) unless block_given?
      return if file_or_glob.blank?
      return if root_path?(file_or_glob)
      depth = options.fetch(:depth, 0)
      max_depth = options.fetch(:max_depth, 0)
      return if depth > MAX_DEPTH || depth > max_depth

      glob_stat(dirname(file_or_glob), depth: depth + 1, max_depth: max_depth, &block)

      dirname = dirname(file_or_glob)
      unless @cache.any?(dirname)
        pattern = root_path?(dirname) ? file_or_glob : File.join(dirname, '*')
        @filesystem.glob_stat(pattern) do |entry|
          @cache.put(entry.name, entry)
        end
      end

      file_regexp = glob_to_regexp(file_or_glob, options)
      @cache.get(dirname).each do |entry|
        next if entry.name == dirname
        next unless entry.name =~ file_regexp
        yield entry
      end if depth.zero?
    end

    # FIXME: cache eviction policy can be more precise
    [:touch!, :mkdir!, :copy_file_to_file, :copy_file_to_dir, :copy_dir, :remove_file, :remove_dir, :move_file_to_file, :move_file_to_dir, :move_dir, :write].each do |method|
      define_method(method) do |*args|
        clear!
        @filesystem.send(method, *args)
      end
    end

    class PathCache
      def initialize(filesystem)
        @filesystem = filesystem
        @cache = {}
      end

      def put(path, entry)
        return unless path
        return if @filesystem.root_path?(path)
        put(File.join(@filesystem.dirname(path), '.'), OpenStruct.new(name: @filesystem.dirname(path)))
        paths = path_split(path)
        elems = paths.reverse.inject(entry) { |a, e| { e => a } }
        @cache.deep_merge!(elems)
      end

      def get(path)
        return unless path
        paths = path_split(path)
        elem = paths.inject(@cache) { |a, e| a.is_a?(Hash) ? a.fetch(e, {}) : a }
        emit(elem)
      rescue KeyError
        EMPTY_SET
      end

      def any?(path)
        elem = get(path)
        return false unless elem
        elem.any? { |entry| entry.name.start_with?(path) }
      end

      private

      def emit(elem)
        return Set.new(to_enum(:emit, elem)).flatten unless block_given?
        case elem
        when Array, Set
          elem.each do |e|
            yield emit(e)
          end
        when Hash
          elem.values.each do |e|
            yield emit(e)
          end
        else
          yield elem
        end
      end

      def path_split(path)
        path.split('/').reject(&:blank?)
      end
    end
  end
end
