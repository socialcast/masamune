# From: http://stackoverflow.com/a/6407200
module Masamune
  class MultiIO
    def initialize(*targets)
      @targets = targets
      @targets.each { |t| t.sync = true }
    end

    def write(*args)
      @targets.each { |t| t.write(*args) }
    end

    def close
      @targets.each(&:close)
    end
  end
end
