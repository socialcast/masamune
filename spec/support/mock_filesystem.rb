class MockFilesystem
  def initialize
    @files = []
  end

  def has_path?(*a)
    false
  end

  def add_path(*a)
    false
  end

  def get_path(*a)
    false
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
    list = []
    entries.select { |elem| elem =~ Regexp.compile(pattern) }.each do |elem|
      if block_given?
        yield elem
      else
        list << elem
      end
    end
    unless block_given?
      list
    end
  end
end
