#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

require 'delegate'

class Masamune::MockFilesystem < Delegator
  def initialize
    @filesystem = Masamune::Filesystem.new
    @filesystem.add_path :root_dir, File.expand_path('../../../', __FILE__)
    @files = {}
  end

  def touch!(*args)
    opts = args.last.is_a?(Hash) ? args.pop : {}
    args.each do |file|
      parent_paths(file).each do |parent|
        @files[parent] = OpenStruct.new(opts.merge(name: parent))
      end
      @files[file] = OpenStruct.new(opts.merge(name: file))
    end
  end

  def exists?(file)
    @files.keys.any? { |path| file == path || path.start_with?(File.join(file, '/')) }
  end

  def glob(pattern, options = {})
    return Set.new(to_enum(:glob, pattern, options)) unless block_given?
    file_regexp = glob_to_regexp(pattern)
    @files.keys.each do |name|
      next if name == dirname(pattern)
      next unless name =~ file_regexp
      yield name
    end
  end

  def glob_sort(pattern, options = {})
    return to_enum(:glob_sort, pattern, options).to_a unless block_given?
    glob(pattern) do |file|
      yield file
    end
  end

  def glob_stat(pattern, options = {})
    return Set.new(to_enum(:glob_stat, pattern, options)) unless block_given?
    file_regexp = glob_to_regexp(pattern, recursive: true)
    @files.each do |_name, stat|
      next if stat.name == dirname(pattern)
      next unless stat.name =~ file_regexp
      yield stat
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

  def clear!; end

  def check_immutable_path!(_file)
    true
  end

  [:mkdir!, :copy_file_to_file, :copy_file_to_dir, :copy_dir, :remove_file, :remove_dir, :move_file_to_file, :move_file_to_dir, :move_dir, :write].each do |method|
    define_method(method) do |*_args|
      # Empty
    end
  end

  def __getobj__
    @filesystem
  end

  def __setobj__(obj)
    @filesystem = obj
  end
end
