require 'masamune/has_environment'

module Masamune
  class Filesystem
    include Masamune::HasEnvironment
    include Masamune::Accumulate
    include Masamune::Actions::S3Cmd
    include Masamune::Actions::HadoopFilesystem

    FILE_MODE = 0777 - File.umask

    def initialize
      @paths = {}
      @immutable_paths = {}
    end

    def add_path(symbol, path, options = {})
      options ||= {}
      options.symbolize_keys!
      eager_path = eager_load_path path
      @paths[symbol.to_sym] = [eager_path, options]
      mkdir!(eager_path) if options[:mkdir]
      add_immutable_path(eager_path) if options[:immutable]
      self
    end

    def get_path(symbol, *extra)
      lazy_path = lambda do |fs|
        fs.has_path?(symbol) or raise "Path :#{symbol} not defined"
        path, options = fs.paths[symbol]

        mkdir!(path) if options[:mkdir]
        expand_params(fs, extra.any? ? File.join(path, extra) : path)
      end

      if eager_load_paths?
        eager_load_path lazy_path.call(self)
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

    def expand_params(fs, path)
      new_path = path.dup
      fs.environment.configuration.params.each do |key, value|
        new_path.gsub!("%#{key.to_s}", value.to_s)
      end
      new_path
    end

    def parent_paths(path, &block)
      if prefix = remote_prefix(path)
        node = path.split(prefix).last
      else
        prefix = ''
        node = path
      end

      return if prefix.blank? && node.blank?
      parent_paths = node ? File.expand_path(node, '/').split('/') : []
      parent_paths.reject! { |x| x.blank? }
      parent_paths.prepend('/') if node =~ %r{\A/}
      tmp = []
      parent_paths.each do |part|
        tmp << part
        current_path = prefix + File.join(tmp)
        break if current_path == path
        yield current_path
      end
    end
    method_accumulate :parent_paths

    def root_path?(path)
      raise ArgumentError, 'path cannot be nil' if path.nil?
      raise ArgumentError, 'path cannot be blank' if path.blank?
      parent_paths(path).length < 1
    end

    def resolve_file(paths = [])
      Array.wrap(paths).select { |path| File.exists?(path) && File.file?(path) }.first
    end

    def dirname(path)
      parent_paths(path).last || path
    end

    def basename(path)
      return unless path
      node = remote_prefix(path) ? path.split(remote_prefix(path)).last : path
      return if node.nil? || node.blank?
      node.split('/').last
    end

    def touch!(*files)
      files.group_by { |path| type(path) }.each do |type, file_set|
        mkdir!(*file_set.map { |file| File.dirname(file) }) unless type == :s3
        case type
        when :hdfs
          hadoop_fs('-touchz', *file_set)
        when :s3
          empty = Tempfile.new('masamune')
          file_set.each do |file|
            s3cmd('put', empty.path, s3b(file, dir: false))
          end
        when :local
          FileUtils.touch(file_set, file_util_args)
        end
      end
    end

    def exists?(file)
      case type(file)
      when :hdfs
        hadoop_fs('-test', '-e', file, safe: true).success?
      when :s3
        s3cmd('ls', s3b(file), safe: true).present?
      when :local
        File.exists?(file)
      end
    end

    def stat(pattern, &block)
      case type(pattern)
      when :hdfs
        hadoop_fs('-ls', pattern, safe: true) do |line|
          next if line =~ /\AFound \d+ items/
          size, date, time, name = line.split(/\s+/).last(4)
          next unless size && date && time && name
          yield OpenStruct.new(name: name, mtime: Time.parse("#{date} #{time} +0000").at_beginning_of_minute.utc, size: size.to_i)
        end
      when :s3
        file_glob, file_regexp = glob_split(pattern)
        s3cmd('ls', '--recursive', s3b(file_glob), safe: true) do |line|
          next if line =~ /\$folder$/
          date, time, size, name = line.split(/\s+/)
          next unless size && date && time && name
          next unless name =~ file_regexp
          yield OpenStruct.new(name: name, mtime: Time.parse("#{date} #{time} +0000").at_beginning_of_minute.utc, size: size.to_i)
        end
      when :local
        Dir.glob(pattern.gsub(%r{/\*\Z}, '/**/*')) do |file|
          stat = File.stat(file)
          yield OpenStruct.new(name: file, mtime: stat.mtime.at_beginning_of_minute.utc, size: stat.size.to_i)
        end
      end
    end
    method_accumulate :stat

    def mkdir!(*dirs)
      dirs.group_by { |path| type(path) }.each do |type, dir_set|
        case type
        when :hdfs
          hadoop_fs('-mkdir', '-p', *dir_set)
        when :s3
          touch! *dir_set.map { |dir| File.join(dir, '.not_empty') }
        when :local
          FileUtils.mkdir_p(dir_set, file_util_args)
        end
      end
    end

    def glob(pattern, &block)
      case type(pattern)
      when :hdfs
        file_glob, file_regexp = glob_split(pattern)
        hadoop_fs('-ls', pattern, safe: true) do |line|
          next if line =~ /\AFound \d+ items/
          name = line.split(/\s+/).last
          next unless name && name =~ file_regexp
          yield q(pattern, name)
        end
      when :s3
        file_glob, file_regexp = glob_split(pattern)
        s3cmd('ls', '--recursive', s3b(file_glob), safe: true) do |line|
          next if line =~ /\$folder$/
          name = line.split(/\s+/).last
          next unless name && name =~ file_regexp
          yield q(pattern, name)
        end
      when :local
        Dir.glob(pattern.gsub(%r{/\*\Z}, '/**/*')) do |file|
          yield file
        end
      end
    end
    method_accumulate :glob

    def glob_sort(pattern, options = {})
      result = glob(pattern)
      case options[:order]
      when :basename
        result.sort { |x,y| File.basename(x) <=> File.basename(y) }
      else
        result
      end
    end

    def copy_file_to_file(src, dst)
      check_immutable_path!(dst)
      mkdir!(dirname(dst)) unless type(dst) == :s3
      copy_file_helper(src, dst, false)
    end

    def copy_file_to_dir(src, dst)
      check_immutable_path!(dst)
      mkdir!(dst) unless type(dst) == :s3
      copy_file_helper(src, dst, true)
    end

    def copy_dir(src, dst)
      check_immutable_path!(dst)
      case [type(src), type(dst)]
      when [:hdfs, :hdfs]
        copy_file_to_dir(src, dst)
      when [:hdfs, :local]
        copy_file_to_dir(src, dst)
      when [:hdfs, :s3]
        copy_file_to_dir(src, dst)
      when [:s3, :s3]
        s3cmd('cp', '--recursive', s3b(src, dir: true), s3b(dst, dir: true))
      when [:s3, :local]
        fixed_dst = File.join(dst, src.split('/')[-1])
        FileUtils.mkdir_p(fixed_dst, file_util_args)
        s3cmd('get', '--recursive', '--skip-existing', s3b(src, dir: true), fixed_dst)
      when [:s3, :hdfs]
        copy_file_to_dir(src, dst)
      when [:local, :local]
        FileUtils.mkdir_p(dst, file_util_args)
        FileUtils.cp_r(src, dst, file_util_args)
      when [:local, :hdfs]
        copy_file_to_dir(src, dst)
      when [:local, :s3]
        s3cmd('put', '--recursive', src, s3b(dst, dir: true))
      end
    end

    def remove_file(file)
      check_immutable_path!(file)
      case type(file)
      when :hdfs
        hadoop_fs('-rm', file)
      when :s3
        s3cmd('del', s3b(file, dir: false))
      when :local
        FileUtils.rm(file, file_util_args)
      end
    end

    def remove_dir(dir)
      raise "#{dir} is root path, cannot remove" if root_path?(dir)
      check_immutable_path!(dir)
      case type(dir)
      when :hdfs
        hadoop_fs('-rmr', dir)
      when :s3
        s3cmd('del', '--recursive', s3b(dir, dir: true))
        s3cmd('del', '--recursive', s3b("#{dir}_$folder$"))
      when :local
        FileUtils.rmtree(dir, file_util_args)
      end
    end

    def move_file_to_file(src, dst)
      check_immutable_path!(src)
      mkdir!(dirname(dst)) unless type(dst) == :s3
      move_file_helper(src, dst, false)
    end

    def move_file_to_dir(src, dst)
      check_immutable_path!(src)
      mkdir!(dst) unless type(dst) == :s3
      move_file_helper(src, dst, true)
    end

    def move_dir(src, dst)
      check_immutable_path!(src)
      case [type(src), type(dst)]
      when [:hdfs, :hdfs]
        move_file_to_file(src, dst)
      when [:hdfs, :local]
        copy_file_to_dir(src, dst)
        remove_dir(src)
      when [:s3, :s3]
        s3cmd('mv', '--recursive', d(src), f(dst))
      when [:s3, :local]
        s3cmd('get', '--recursive', d(src), f(dst))
        remove_dir(src)
      when [:s3, :hdfs]
        copy_file_to_dir(src, dst)
        remove_dir(src)
      when [:hdfs, :s3]
        copy_file_to_dir(src, d(dst))
        remove_dir(src)
      when [:local, :local]
        move_file_to_file(src, dst)
      when [:local, :hdfs]
        move_file_to_file(src, dst)
      when [:local, :s3]
        s3cmd('put', '--recursive', d(src), d(dst))
        remove_dir(src)
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
        buf.rewind
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

    def chown!(*files)
      opts = files.last.is_a?(Hash) ? files.pop : {}
      user, group = opts.fetch(:user, current_user), opts.fetch(:group, current_group)

      files.group_by { |path| type(path) }.each do |type, file_set|
        case type
        when :hdfs
          hadoop_fs('-chown', '-R', [user, group].compact.join(':'), *file_set)
        when :s3
          # NOTE intentionally skip
        when :local
          FileUtils.chown_R(user, group, file_set, file_util_args)
        end
      end
    end

    def mktemp!(path)
      get_path(path, SecureRandom.base64).tap do |file|
        touch!(file)
      end
    end

    def glob_split(input)
      [ input.include?('*') ? input.split('*').first + '*' : input, glob_to_regexp(input) ]
    end

    def glob_to_regexp(input)
      /\A#{Regexp.escape(input).gsub('\\*', '.*?')}\z/
    end

    private

    def eager_load_path(path)
      case path
      when String
        path
      when Proc
        path.call(self)
      else
        raise "Unknown path #{path.inspect}"
      end
    end

    def remote_prefix(dir)
      dir[%r{\As3n?://.*?(?=/)}] ||
      dir[%r{\As3n?://.*?\Z}] ||
      dir[%r{\Afile://}] ||
      dir[%r{\Ahdfs://}]
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

    def file_util_args
      {noop: configuration.no_op, verbose: configuration.verbose}
    end

    def qualify_file(dir, file)
      if prefix = remote_prefix(dir) and file !~ /\A#{Regexp.escape(prefix)}/
        "#{prefix}/#{file.sub(%r{\A/+}, '')}"
      else
        file
      end
    end
    alias :q :qualify_file

    def ensure_dir(dir)
      File.join(dir, '/')
    end
    alias :d :ensure_dir

    def ensure_file(file)
      file.chomp('/')
    end
    alias :f :ensure_file

    def add_immutable_path(path)
      @immutable_paths[path] = /\A#{Regexp.escape(path)}/
    end

    def check_immutable_path!(file)
      @immutable_paths.each do |path, regex|
        raise "#{path} is marked as immutable, cannot modify #{file}" if file[regex].present?
      end
    end

    def current_user
      Etc.getlogin
    end

    def current_group
      Etc.getgrgid(Etc.getpwnam(current_user).gid).name
    rescue
    end

    def copy_file_helper(src, dst, dir)
      case [type(src), type(dst)]
      when [:hdfs, :hdfs]
        hadoop_fs('-cp', src, dst)
      when [:hdfs, :local]
        hadoop_fs('-copyToLocal', src, dst)
      when [:hdfs, :s3]
        hadoop_fs('-cp', src, s3n(dst))
      when [:s3, :s3]
        s3cmd('cp', src, s3b(dst, dir: dir))
      when [:s3, :local]
        s3cmd('get', src, dst)
      when [:s3, :hdfs]
        hadoop_fs('-cp', s3n(src), dst)
      when [:local, :local]
        FileUtils.cp(src, dst, file_util_args)
      when [:local, :hdfs]
        hadoop_fs('-copyFromLocal', src, dst)
      when [:local, :s3]
        s3cmd('put', src, s3b(dst, dir: dir))
      end
    end

    def move_file_helper(src, dst, dir)
      case [type(src), type(dst)]
      when [:hdfs, :hdfs]
        hadoop_fs('-mv', src, dst)
      when [:hdfs, :local]
        # FIXME use hadoop_fs('-moveToLocal', src, dst) if implemented
        hadoop_fs('-copyToLocal', src, dst)
        hadoop_fs('-rm', src)
      when [:hdfs, :s3]
        copy_file_to_file(src, s3n(dst, dir: dir))
        hadoop_fs('-rm', src)
      when [:s3, :s3]
        s3cmd('mv', src, s3b(dst, dir: dir))
      when [:s3, :local]
        s3cmd('get', src, dst)
        s3cmd('del', src)
      when [:s3, :hdfs]
        hadoop_fs('-mv', s3n(src), dst)
      when [:local, :local]
        FileUtils.mv(src, dst, file_util_args)
        FileUtils.chmod(FILE_MODE, dst, file_util_args)
      when [:local, :hdfs]
        hadoop_fs('-moveFromLocal', src, dst)
      when [:local, :s3]
        s3cmd('put', src, s3b(dst, dir: dir))
        FileUtils.rm(src, file_util_args)
      end
    end
  end
end
