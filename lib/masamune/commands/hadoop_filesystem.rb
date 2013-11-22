require 'masamune/has_context'

module Masamune::Commands
  class HadoopFilesystem
    include Masamune::HasContext

    DEFAULT_ATTRIBUTES =
    {
      :path         => 'hadoop',
      :options      => [],
      :extra        => [],
      :block        => nil,
      :print        => false
    }

    def initialize(attrs = {})
      DEFAULT_ATTRIBUTES.merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    def command_args
      args = []
      args << @path
      args << 'fs'
      args << @options.map(&:to_a)
      args << @extra
      args.flatten
    end

    def handle_stdout(line, line_no)
      @block.call(line) if @block
      print(line) if @print
    end
  end
end
