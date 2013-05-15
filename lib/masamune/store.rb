require 'pstore'

module Masamune
  class Store
    include Masamune::Actions::Filesystem

    def initialize(store_name)
      @pstore = PStore.new(File.join(fs[:build_dir], store_name.to_s))
    end

    def [](key)
      @pstore.transaction { @pstore[key] }
    end

    def []=(key, val)
      @pstore.transaction { @pstore[key]  = val } unless Masamune.configuration.dryrun
    end
  end
end
