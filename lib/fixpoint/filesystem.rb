class Fixpoint::Filesystem
  extend Forwardable

  def initialize
    @files = []
    @locations = {}
  end

  def touch!(*files)
    @files += files
  end

  def exists?(file)
    @files.include?(file)
  end

  def entries
    @files
  end

  def add_location(symbol, value)
    @locations[symbol] = value
  end

  def_delegators :@locations, :[]
end
