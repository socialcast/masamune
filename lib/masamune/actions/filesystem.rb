require 'active_support/concern'

module Masamune::Actions
  module Filesystem
    extend ActiveSupport::Concern

    def filesystem
      defined?(context) ? context.filesystem : Masamune.filesystem
    end
    alias :fs :filesystem

    included do |base|
      base.extend(Filesystem)
    end
  end
end
