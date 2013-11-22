module Masamune::Actions
  module Filesystem
    def filesystem
      context.filesystem
    end
    alias :fs :filesystem

    def self.included(base)
      base.extend(self)
    end
  end
end
