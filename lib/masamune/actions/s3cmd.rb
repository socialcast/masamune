module Masamune::Actions
  module S3Cmd
    include Masamune::Actions::Common

    def s3_sync(src, dst)
      execute('s3cmd', 'sync', src, dst, :dir => true)
    end
  end
end
