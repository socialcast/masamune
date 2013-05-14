class Masamune::Filesystem::Hadoop < Masamune::Filesystem
  include Masamune::Actions::Common

  def glob(pattern, &block)
    execute('hadoop', 'fs', '-ls', pattern) do |entry|
      yield entry.split(/\s+/).last
    end
  end

  def copy_file(src, dst)
    execute('hadoop', 'fs', '-cp', src, dst)
  end

  def exists?(file)
    execute('hadoop', 'fs', '-test', '-e', file).success?
  end

  def remove_dir(dir)
    execute('hadoop', 'fs', '-rmr', dir)
  end
end
