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
      @cache = {}
    end

    def exists?(file)
      glob(file).any?
    end

    def stat(file_or_glob, &block)
      update!(file_or_glob)
      file_regexp = glob_to_regexp(file_or_glob)
      @cache.keys.each do |file|
        if file =~ file_regexp
          @cache[file] ||= @filesystem.stat(file)
          yield @cache[file]
        end
      end
    end
    method_accumulate :stat

    def glob(file_or_glob, &block)
      update!(file_or_glob)
      file_regexp = glob_to_regexp(file_or_glob)
      @cache.keys.each do |file|
        yield file if file =~ file_regexp
      end
    end
    method_accumulate :glob

    # FIXME cache eviction policy can be more precise
    [:touch!, :mkdir!, :copy_file_to_file, :copy_file_to_dir, :copy_dir, :remove_file, :remove_dir, :move_file, :move_dir, :write, :glob_sort].each do |meth|
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

    def update!(file_or_glob, expand = true, &block)
      return if file_or_glob.blank?

      dirname = @filesystem.dirname(file_or_glob)
      return if @cache.key?(dirname)
      @filesystem.stat(File.join(dirname, '*')) do |entry|
        @filesystem.parent_paths(entry.name) { |path| @cache[path] ||= nil }
        @cache[entry.name] = entry
      end
      update!(dirname, false) if expand && !@filesystem.root_path?(dirname)
    end
  end
end
