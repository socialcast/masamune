module Masamune
  class Filesystem
    include Masamune::Accumulate
    include Masamune::Actions::S3Cmd
    include Masamune::Actions::Execute

    def initialize
      @paths = {}
    end

    def add_path(symbol, path, options = {})
      eager_path = eager_load_path path
      @paths[symbol] = [eager_path, options]
      mkdir!(eager_path) if options[:mkdir]
      self
    end

    def get_path(symbol, *extra)
      lazy_path = lambda do
        @paths.has_key?(symbol) or raise "Path :#{symbol} not defined"
        path, options = @paths[symbol]
        mkdir!(path) if options[:mkdir]
        if extra.any?
          File.join(path, extra)
        else
          path
        end
      end

      if eager_load_paths?
        eager_load_path lazy_path.call
      else
        lazy_path
      end
    end
    alias :path :get_path

    def has_path?(symbol)
      @paths.has_key?(symbol)
    end

    def paths
      @paths
    end

    def resolve_file(paths = [])
      Array.wrap(paths).select { |path| File.exists?(path) && File.file?(path) }.first
    end

    def touch!(*files)
      files.group_by { |path| type(path) }.each do |type, file_set|
        case type
        when :hdfs
          execute_hadoop_fs('-touchz', *file_set)
        when :s3
          # NOTE intentionally skip
        else
          FileUtils.touch(*file_set, file_util_args)
        end
      end
    end

    def exists?(file)
      case type(file)
      when :hdfs
        execute_hadoop_fs('-test', '-e', file, safe: true).success?
      when :s3
        glob(file).present?
      else
        File.exists?(file)
      end
    end

    def mkdir!(*dirs)
      dirs.group_by { |path| type(path) }.each do |type, dir_set|
        case type
        when :hdfs
          execute_hadoop_fs('-mkdir', *dir_set)
        when :s3
          # NOTE intentionally skip
        else
          FileUtils.mkdir_p(*dir_set, file_util_args)
        end
      end
    end

    def glob(pattern, &block)
      case type(pattern)
      when :hdfs
        execute_hadoop_fs('-ls', pattern, safe: true) do |line|
          next if line =~ /\AFound \d+ items/
          yield q(pattern, line.split(/\s+/).last)
        end
      when :s3
        head_glob, *tail_glob = pattern.split('*')
        tail_regexp = Regexp.compile(tail_glob.map { |glob| Regexp.escape(glob) }.join('.*?') + '\z')
        s3cmd('ls', s3b(head_glob + '*'), safe: true) do |line|
          next if line =~ /\$folder$/
          next unless line =~ tail_regexp
          yield q(pattern, line.split(/\s+/).last)
        end
      else
        Dir.glob(pattern) do |file|
          yield file
        end
      end
    end
    method_accumulate :glob

    def copy_file(src, dst)
      mkdir!(dst)
      case [type(src), type(dst)]
      when [:hdfs, :hdfs]
        execute_hadoop_fs('-cp', src, dst)
      when [:hdfs, :local]
        execute_hadoop_fs('-copyToLocal', src, dst)
      when [:hdfs, :s3]
        execute_hadoop_fs('-cp', src, s3n(dst))
      when [:s3, :s3]
        s3cmd('cp', src, s3b(dst, dir: true))
      when [:s3, :local]
        s3cmd('get', src, dst)
      when [:s3, :hdfs]
        execute_hadoop_fs('-cp', s3n(src), dst)
      when [:local, :local]
        FileUtils.cp(src, dst, file_util_args)
      when [:local, :hdfs]
        execute_hadoop_fs('-copyFromLocal', src, dst)
      when [:local, :s3]
        s3cmd('put', src, s3b(dst, dir: true))
      end
    end

    def remove_dir(dir)
      # FIXME never rm blank or slash
      case type(dir)
      when :hdfs
        execute_hadoop_fs('-rmr', dir)
      when :s3
        s3cmd('del', '--recursive', s3b(dir, dir:true))
        s3cmd('del', '--recursive', s3b("#{dir}_$folder$"))
      else
        FileUtils.rmtree(dir, file_util_args)
      end
    end

    def move_file(src, dst)
      mkdir!(File.dirname(dst))
      case [type(src), type(dst)]
      when [:hdfs, :hdfs]
        execute_hadoop_fs('-mv', src, dst)
      when [:hdfs, :local]
        # FIXME use execute_hadoop_fs('-moveToLocal', src, dst) if implemented
        execute_hadoop_fs('-copyToLocal', src, dst)
        execute_hadoop_fs('-rm', src)
      when [:hdfs, :s3]
        execute_hadoop_fs('-mv', src, s3n(dst))
      when [:s3, :s3]
        s3cmd('mv', src, dst)
      when [:s3, :local]
        s3cmd('get', src, dst)
        s3cmd('del', src)
      when [:s3, :hdfs]
        execute_hadoop_fs('-mv', s3n(src), dst)
      when [:local, :local]
        FileUtils.mv(src, dst, file_util_args)
      when [:local, :hdfs]
        execute_hadoop_fs('-moveFromLocal', src, dst)
      when [:local, :s3]
        s3cmd('put', src, dst)
        FileUtils.rm(src, file_util_args)
      end
    end

    def cat(*files)
      StringIO.new.tap do |buf|
        files.group_by { |path| type(path) }.each do |type, file_set|
          case type
          when :local
            file_set.map do |file|
              next unless File.exists?(file)
              next if File.directory?(file)
              buf << File.read(file)
            end
          end
        end
      end
    end

    def write(buf, dst)
      case type(dst)
      when :local
        mkdir!(File.dirname(dst))
        File.open(dst, 'w') do |file|
          file.write buf
        end
      end
    end

    private

    def eager_load_path(path)
      case path
      when String
        path
      when Proc
        path.call
      else
        raise "Unknown path #{path.inspect}"
      end
    end

    def eager_load_paths?
      @paths.reject { |key,_| key == :root_dir }.any?
    end

    def type(path)
      case path
      when %r{\Afile://}, %r{\Ahdfs://}
        :hdfs
      when %r{\As3n?://}
        :s3
      else
        :local
      end
    end

    def hadoop_fs_command(options = {})
      args = []
      args << Masamune.configuration.hadoop_filesystem[:path]
      args << 'fs'
      args << Masamune.configuration.hadoop_filesystem[:options].map(&:to_a)
      args.flatten
    end

    def file_util_args
      {noop: Masamune.configuration.no_op, verbose: Masamune.configuration.verbose}
    end

    def execute_hadoop_fs(*args, &block)
      if block_given?
        execute(*hadoop_fs_command, *args) do |line, line_no|
          yield line
        end
      else
        execute(*hadoop_fs_command, *args)
      end
    end

    def qualify_file(dir, file)
      if prefix = remote_prefix(dir) and file !~ /\A#{Regexp.escape(prefix)}/
        prefix + file.sub(%r{\A/+}, '')
      else
        file
      end
    end
    alias :q :qualify_file

    def remote_prefix(dir)
      dir[%r{s3n?://.*?/}] ||
      dir[%r{file:///}] ||
      dir[%r{hdfs:///}]
    end
  end
end
