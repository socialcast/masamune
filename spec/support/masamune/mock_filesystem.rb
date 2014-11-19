require 'delegate'

class Masamune::MockFilesystem < Delegator
  include Masamune::Accumulate

  def initialize
    @filesystem = Masamune::Filesystem.new
    @filesystem.add_path :root_dir, File.expand_path('../../../', __FILE__)
    @files = {}
  end

  def touch!(*args)
    opts = args.last.is_a?(Hash) ? args.pop : {}
    args.each do |file|
      @files[file] = OpenStruct.new(opts.merge(name: file))
    end
  end

  def exists?(file)
    @files.keys.include?(file)
  end

  def glob(pattern, &block)
    file_regexp = glob_to_regexp(pattern)
    @files.keys.each do |name|
      yield name if name =~ file_regexp
    end
  end
  method_accumulate :glob

  def glob_sort(pattern, options = {})
    glob(pattern)
  end

  def glob_stat(pattern, &block)
    file_regexp = glob_to_regexp(pattern)
    @files.each do |name, stat|
      yield stat if name =~ file_regexp
    end
  end

  def stat(file)
    @files[file]
  end

  def write(data, file)
    @files[file] = OpenStruct.new(name: file, data: data)
  end

  def cat(file)
    @files[file].data
  end

  def clear!
  end

  def check_immutable_path!(file)
    true
  end

  def __getobj__
    @filesystem
  end

  def __setobj__(obj)
    @filesystem = obj
  end
end
