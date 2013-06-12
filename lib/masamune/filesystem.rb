module Masamune
  class Filesystem
    include Masamune::Actions::Execute

    def initialize
      @paths = {}
    end

    def add_path(symbol, path, options = {})
      @paths[symbol] = [path, options]
      mkdir!(path) if options[:mkdir]
      self
    end

    def get_path(symbol, *extra)
      @paths.has_key?(symbol) or raise "Path :#{symbol} not defined"
      path, options = @paths[symbol]
      mkdir!(path) if options[:mkdir]
      if extra.any?
        File.join(path, extra)
      else
        path
      end
    end
    alias :path :get_path

    def has_path?(symbol)
      @paths.has_key?(symbol)
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
      if block_given?
        glob_with_block(pattern) do |file|
          yield file
        end
      else
        [].tap do |result|
          glob_with_block(pattern) do |file|
            result << file
          end
        end
      end
    end

    # TODO local, hdfs permutations
    def copy_file(src, dst)
      mkdir!(dst)
      case [type(src), type(dst)]
      when [:hdfs, :hdfs]
        execute_hadoop_fs('-cp', src, dst)
      when [:s3, :s3]
        execute('s3cmd', 'cp', src, dst)
      when [:local, :local]
        FileUtils.cp(src, dst, file_util_args)
      end
    end

    def remove_dir(dir)
      # FIXME never rm blank or slash
      case type(dir)
      when :hdfs
        execute_hadoop_fs('-rmr', dir)
      when :s3
        execute('s3cmd', 'del', '--recursive', s3b(dir, dir:true))
      else
        FileUtils.rmtree(dir, file_util_args)
      end
    end

    # TODO round out permutations
    def move_file(src, dst)
      mkdir!(File.dirname(dst))
      case [type(src), type(dst)]
      when [:hdfs, :hdfs]
        execute_hadoop_fs('-mv', src, dst)
      when [:s3, :s3]
        execute('s3cmd', 'mv', src, dst)
      when [:local, :s3]
        execute('s3cmd', 'put', src, dst)
      when [:local, :local]
        FileUtils.mv(src, dst, file_util_args)
      end
    end

    def cat(*files)
      StringIO.new.tap do |buf|
        files.group_by { |path| type(path) }.each do |type, file_set|
          case type
          when :local
            file_set.map do |file|
              buf << File.read(file)
            end
          end
        end
      end
    end

    def write(buf, src)
      case type(src)
      when :local
        File.open(src, 'w') do |file|
          file.write buf
        end
      end
    end

    private

    def glob_with_block(pattern, &block)
      case type(pattern)
      when :hdfs
        execute_hadoop_fs('-ls', pattern, safe: true) do |line|
          next if line =~ /\AFound \d+ items/
          yield q(pattern, line.split(/\s+/).last)
        end
      when :s3
        head_glob, *tail_glob = pattern.split('*')
        tail_regexp = Regexp.compile(tail_glob.map { |glob| Regexp.escape(glob) }.join('.*?') + '\z')
        execute('s3cmd', 'ls', s3b(head_glob + '*'), safe: true) do |line|
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

    def hadoop_fs_args(options = {})
      args = []
      args << Masamune.configuration.command_options[:hadoop_fs].call
      args.flatten
    end

    def file_util_args
      {noop: Masamune.configuration.no_op, verbose: Masamune.configuration.verbose}
    end

    def execute_hadoop_fs(*args, &block)
      if block_given?
        execute('hadoop', 'fs', *hadoop_fs_args, *args) do |line, line_no|
          yield line
        end
      else
        execute('hadoop', 'fs', *hadoop_fs_args, *args)
      end
    end

    def qualify_file(dir, file)
      if prefix = dir[%r{s3n?://.*?/}] and file !~ /\A#{Regexp.escape(prefix)}/
        File.join(prefix, file)
      else
        file
      end
    end
    alias :q :qualify_file

    module ClassMethods
      def s3n(file, options = {})
        file.dup.tap do |out|
          out.sub!(%r{\As3://}, 's3n://')
          out.sub!(%r{/?\z}, '/') if options[:dir]
        end
      end

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
