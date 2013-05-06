module Fixpoint::Actions
  module Filesystem
    def filesystem
      Fixpoint::configuration.filesystem
    end
    alias :fs :filesystem

    def self.included(base)
      base.extend(self)
    end
  end
end
