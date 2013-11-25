require 'active_support/concern'

module Masamune::Actions
  module Filesystem
    extend ActiveSupport::Concern

    module ClassMethods
      def filesystem
        defined?(context) ? context.filesystem : Masamune.context.filesystem
      end
      alias :fs :filesystem
    end
    include ClassMethods
  end
end
