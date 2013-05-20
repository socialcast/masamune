module Masamune::Actions
  module Path
    def path(symbol)
      Masamune::configuration.path_resolver.get_path(symbol)
    end

    def self.included(base)
      base.extend(self)
    end
  end
end
