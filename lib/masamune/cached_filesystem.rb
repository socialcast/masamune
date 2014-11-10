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
      @cache.key?(file) || glob(file).any?
    end

    def stat(file_or_glob, &block)
      scan(file_or_glob) do |entry|
        yield entry
      end
    end
    method_accumulate :stat

    def glob(file_or_glob, &block)
      scan(file_or_glob) do |entry|
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

    def scan(file_or_glob, &block)
      return if file_or_glob.blank?

      dirname = @filesystem.dirname(file_or_glob)
      unless @cache.key?(dirname)
        @filesystem.stat(File.join(dirname, '*')) do |entry|
          @cache[dirname] << entry
        end
      end

      file_regexp = glob_to_regexp(file_or_glob, recursive: false)
      @cache[dirname].each do |entry|
        yield entry if entry.name =~ file_regexp
      end
    end
  end
end
