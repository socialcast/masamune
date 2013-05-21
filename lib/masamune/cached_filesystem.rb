require 'delegate'

module Masamune
  class CachedFilesystem < Delegator
    def initialize(filesystem)
      super
      @filesystem = filesystem
      @paths = []
    end

    def exists?(file)
      unless @paths.include?(file)
        path = file.split('/')
        dirname, basename = path[0 .. -2].join('/'), path[-1]
        @paths += glob(File.join(dirname, '*'))
      end
      @paths.include?(file)
    end

    def __getobj__
      @filesystem
    end

    def __setobj__(obj)
      @filesystem = obj
    end
  end
end
