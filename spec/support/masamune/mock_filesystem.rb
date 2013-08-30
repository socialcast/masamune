require 'delegate'

class Masamune::MockFilesystem < Delegator
  include Masamune::Accumulate

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
    matcher = Regexp.compile(pattern.gsub('*', '.*?'))
    @files.each do |elem|
      yield elem if matcher.match(elem)
    end
  end
  method_accumulate :glob

  def __getobj__
    @filesystem
  end

  def __setobj__(obj)
    @filesystem = obj
  end
end
