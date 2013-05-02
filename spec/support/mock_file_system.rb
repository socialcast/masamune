class MockFileSystem
  def initialize
    @files = []
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
end
