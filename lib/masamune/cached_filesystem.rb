require 'delegate'

module Masamune
  class CachedFilesystem < Delegator
    def initialize(filesystem)
      super
      @filesystem = filesystem
      @paths = []
      @missing = []
    end

    def exists?(file)
      return false if @missing.any? { |re| re.match(file) }
      unless @paths.include?(file)
        path = file.split('/')
        dirname, basename = path[0 .. -2].join('/'), path[-1]
        paths = glob(File.join(dirname, '*'))
        if paths.any?
          @paths += paths
        else
          @missing.push /\A#{Regexp.escape(dirname)}.*/
        end
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
