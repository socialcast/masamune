module Masamune::Actions
  module S3Cmd
    require 'masamune/filesystem'
    require 'masamune/filesystem/s3'
    include Masamune::Actions::Common
    include Masamune::Filesystem::S3::ClassMethods

    def s3_sync(src, dst)
      execute('s3cmd', 'sync', src, s3b(dst))
    end
  end
end
