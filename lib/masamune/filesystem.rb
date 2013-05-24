module Masamune
  class Filesystem
    include Masamune::Actions::Common

    def initialize
      @paths = {}
    end

    def add_path(symbol, path, options = {})
      @paths[symbol] = path
      mkdir!(path) if options[:mkdir]
      self
    end

    def get_path(symbol)
      @paths[symbol] or raise "Path :#{symbol} not defined"
    end
    alias :path :get_path

    # TODO look into FileUtils :dryrun, :noop, :verbose
    def touch!(*files)
      files.group_by { |path| type(path) }.each do |type, file_set|
        case type
        when :hdfs
          execute_hadoop_fs('-touchz', *file_set)
        when :s3
          execute_hadoop_fs('-touchz', *file_set.map { |f| s3n(f) })
        else
          FileUtils.touch(*file_set)
        end
      end
    end

    def exists?(file)
      case type(file)
      when :hdfs
        execute_hadoop_fs('-test', '-e', file, safe: true).success?
      when :s3
        execute_hadoop_fs('-test', '-e', s3n(file), safe: true).success?
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
          execute_hadoop_fs('-mkdir', *dir_set.map { |d| s3n(d, dir: true) })
        else
          FileUtils.mkdir_p(*dir_set)
        end
      end
    end

    def glob(pattern, &block)
      if block_given?
        glob_with_block(pattern) do |file|
          yield file
        end
      else
        result = []
        glob_with_block(pattern) do |file|
          result << file
        end
        result
      end
    end

    def copy_file(src, dst)
      mkdir!(dst)
      case [type(src), type(dst)]
      when [:hdfs, :hdfs]
        execute_hadoop_fs('-cp', src, dst)
      when [:s3, :s3]
        execute('s3cmd', 'cp', src, dst)
      when [:local, :local]
        FileUtils.cp(src, dst)
      end
    end

    def remove_dir(dir)
      case type(dir)
      when :hdfs
        execute_hadoop_fs('-rmr', dir)
      when :s3
        execute_hadoop_fs('-rmr', s3n(dir, dir: true))
      else
        FileUtils.remove_dir(dir, true)
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
        execute_hadoop_fs('-ls', s3n(pattern), safe: true) do |line|
          next if line =~ /\AFound \d+ items/
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

    def execute_hadoop_fs(*args, &block)
      if block_given?
        execute('hadoop', 'fs', *hadoop_fs_args, *args) do |line|
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
