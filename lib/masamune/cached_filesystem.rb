require 'delegate'

module Masamune
  class CachedFilesystem < Delegator
    def initialize(filesystem)
      super
      @filesystem = filesystem
      @paths = Set.new
      @missing = []
    end

    def exists?(file)
      return false if @missing.any? { |re| re.match(file) }
      file_re = /\A#{Regexp.escape(file)}/
      unless @paths.any? { |path| path[file_re] }
        Masamune.logger.debug("MISS #{file}")
        path = file.split('/')
        dirname, basename = path[0 .. -2].join('/'), path[-1]
        paths = glob(File.join(dirname, '*'))
        if paths.any?
          @paths = @paths.union(paths)
        else
          @missing.push /\A#{Regexp.escape(dirname)}.*/
        end
      else
        Masamune.logger.debug("HIT #{file}")
      end
      @paths.any? { |path| path[file_re] }
    end

    def __getobj__
      @filesystem
    end

    def __setobj__(obj)
      @filesystem = obj
    end
  end
end
