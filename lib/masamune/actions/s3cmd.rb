module Masamune::Actions
  module S3Cmd
    require 'masamune/actions/execute'
    require 'masamune/filesystem'
    include Masamune::Filesystem::ClassMethods
    include Masamune::Actions::Execute

    def s3_sync(src, dst)
      execute('s3cmd', 'sync', s3b(src), s3b(dst, :dir => true))
    end
  end
end
