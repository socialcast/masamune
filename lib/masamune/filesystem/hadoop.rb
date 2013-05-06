class Masamune::Filesystem::Hadoop < Masamune::Filesystem
  include Masamune::Actions::Common

  def exists?(file)
    execute('hadoop', 'fs', '-test', '-e', file).success?
  end

  def remove_dir(dir)
    execute('hadoop', 'fs', '-rmr', dir)
  end
end
