require 'delegate'

class MockFilesystem < Delegator
  def initialize
    @filesystem = Masamune::Filesystem.new
    @filesystem.add_path :root_dir, File.expand_path('../../../', __FILE__)
    @files = []
  end

  def touch!(*files)
    @files += files
  end

  def exists?(file)
    @files.include?(file)
  end

  def glob(pattern, &block)
    list = []
    @files.select { |elem| elem =~ Regexp.compile(pattern) }.each do |elem|
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

  def __getobj__
    @filesystem
  end

  def __setobj__(obj)
    @filesystem = obj
  end
end
