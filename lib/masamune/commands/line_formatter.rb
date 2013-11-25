require 'masamune/proxy_delegate'

module Masamune::Commands
  class LineFormatter
    include Masamune::ProxyDelegate

    def initialize(delegate, attrs = {})
      @delegate   = delegate
      @ifs        = attrs.fetch(:ifs, "\001")
      @ofs        = attrs.fetch(:ofs, "\001")
    end

    def handle_stdout(line, line_no)
      if @delegate.respond_to?(:handle_stdout)
        line.gsub!(@ifs, @ofs) if @ifs != @ofs
        @delegate.handle_stdout(line, line_no)
      else
        raise ArgumentError, "delegate #{@delegate.class} must respond_to handle_stdout"
      end
    end
  end
end
