require 'delegate'

module Masamune
  class CachedFilesystem < Delegator
    def initialize(filesystem)
      super
      @filesystem = filesystem
      clear!
    end

    def clear!
      @paths = Set.new
    end

    def exists?(file)
      dirname = dirname(file)
      if @paths.include?(dirname)
        if @paths.include?(file)
          true
        else
          false
        end
      else
        glob(File.join(dirname, '*')).each do |path|
          @paths = @paths.union(subpaths(path))
        end
        @paths.include?(file)
      end
    end

    def dirname(file)
      path = file.split('/')
      dirname, basename = path[0 .. -2].join('/'), path[-1]
      dirname
    end

    def subpaths(file)
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
