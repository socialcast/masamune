class Fixpoint::Filesystem::Hadoop < Fixpoint::Filesystem
  include Fixpoint::Actions::Common

  def exists?(file)
    execute('hadoop', 'fs', '-test', '-e', file).success?
  end

  def remove_dir(dir)
    execute('hadoop', 'fs', '-rmr', dir)
  end
end
