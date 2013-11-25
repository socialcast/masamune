require 'active_support/concern'

module Masamune::Actions
  module Filesystem
    extend ActiveSupport::Concern

    module ClassMethods
      def filesystem
        context.filesystem
      end
      alias :fs :filesystem
    end
    include ClassMethods
  end
end
