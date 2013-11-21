module Masamune::Actions
  module Filesystem
    def filesystem
      client.filesystem
    end
    alias :fs :filesystem

    def self.included(base)
      base.extend(self)
    end
  end
end
