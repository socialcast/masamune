class Masamune::Filesystem::Hadoop < Masamune::Filesystem
  include Masamune::Actions::Common

  def touch!(*files)
    execute_hadoop_fs('-touchz', *files)
  end

  # TODO fast option, ls Dir(file) + '*', keep in memory
  def exists?(file)
    execute_hadoop_fs('-test', '-e', file, :safe => true).success?
  end

  def glob(pattern, &block)
    execute_hadoop_fs('-ls', pattern, :safe => true) do |line|
      next if line =~ /\AFound \d+ items/
      yield q(pattern, line.split(/\s+/).last)
    end
  end

  def copy_file(src, dst)
    execute_hadoop_fs('-cp', src, dst)
  end

  def remove_dir(dir)
    execute_hadoop_fs('-rmr', dir)
  end

  private

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
end
