# From: http://stackoverflow.com/a/6407200
module Masamune
  class MultiIO
    def initialize(*targets)
      @targets = targets
      @targets.each do |t|
        t.sync = true
      end
    end

    def write(*args)
      @targets.each do |t|
        t.write(*args)
      end
    end

    def close
      @targets.each(&:close)
    end
  end
end
