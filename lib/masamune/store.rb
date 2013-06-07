require 'pstore'

# TODO use human readable format like yaml
# TODO sync to s3
module Masamune
  class Store
    include Masamune::Actions::Filesystem

    def initialize(store_name)
      @pstore = PStore.new(File.join(fs.path(:var_dir), store_name.to_s))
    end

    def [](key)
      @pstore.transaction { @pstore[key] }
    end

    def []=(key, val)
      @pstore.transaction { @pstore[key]  = val } unless Masamune.configuration.no_op
    end
  end
end
