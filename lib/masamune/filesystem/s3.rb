require 'masamune/filesystem/hadoop'
module Masamune
  class Filesystem::S3 < Filesystem::Hadoop
    def copy_file(src, dst)
      execute('s3cmd', 'cp', s3b(src), s3b(dst))
    end

    module ClassMethods
      def s3b(file, options = {})
        file.dup.tap do |out|
          out.sub!(%r{\As3n://}, 's3://')
          out.sub!(%r{/?\z}, '/') if options[:dir]
        end
      end
    end

    include ClassMethods
  end
end
