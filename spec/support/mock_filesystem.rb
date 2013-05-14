class MockFilesystem
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

  def glob(pattern, &block)
    entries.select { |elem| elem =~ Regexp.compile(pattern) }.each do |elem|
      yield elem
    end
  end
end
