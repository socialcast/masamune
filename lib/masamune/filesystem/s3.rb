module Masamune
  class Filesystem::S3 < Filesystem::Hadoop
    def copy_file(src, dst)
      execute('s3cmd', 'cp', s3b(src), s3b(dst))
    end

    private

    def s3b(file)
      file.sub(%r{\As3n://}, 's3://')
    end
  end
end
