module Masamune::Commands
  class RetryWithBackoff
    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    DEFAULT_RETRIES = 3
    DEFAULT_BACKOFF = 5

    attr_accessor :retries, :backoff

    def initialize(delegate, opts = {})
      @delegate     = delegate
      self.retries  = opts.fetch(:retries, DEFAULT_RETRIES)
      self.backoff  = opts.fetch(:backoff, DEFAULT_BACKOFF)
      @retry_count  = 0
    end

    def around_execute(&block)
      begin
        if @delegate.respond_to?(:around_execute)
          @delegate.around_execute(&block)
        else
          block.call
        end
      rescue
        sleep backoff
        @retry_count += 1
        retry unless @retry_count > retries
      end
    end

    def proxy_methods
      [:around_execute]
    end
  end
end
