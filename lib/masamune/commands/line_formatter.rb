module Masamune::Commands
  class LineFormatter
    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    attr_accessor :ifs, :ofs

    def initialize(delegate, opts = {})
      @delegate       = delegate
      self.ifs        = opts.fetch(:ifs, "\001")
      self.ofs        = opts.fetch(:ofs, "\001")
    end

    def handle_stdout(line, line_no)
      if @delegate.respond_to?(:handle_stdout)
        line.gsub!(ifs, ofs) if ifs != ofs
        @delegate.handle_stdout(line, line_no)
      else
        raise ArgumentError, "delegate #{@delegate.class} must respond_to handle_stdout"
      end
    end

    def proxy_methods
      [:handle_stdout]
    end
  end
end
