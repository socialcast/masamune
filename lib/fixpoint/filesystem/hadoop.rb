class Fixpoint::Filesystem::Hadoop < Fixpoint::Filesystem
  include Fixpoint::Actions::Common

  def exists?(file)
    execute('hadoop', 'fs', '-test', '-e', file).success?
  end
end
