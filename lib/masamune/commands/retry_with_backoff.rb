require 'masamune/proxy_delegate'

module Masamune::Commands
  class RetryWithBackoff
    include Masamune::ProxyDelegate

    DEFAULT_RETRIES = 3
    DEFAULT_BACKOFF = 5

    def initialize(delegate, attrs = {})
      @delegate     = delegate
      @retries      = attrs.fetch(:retries, DEFAULT_RETRIES)
      @backoff      = attrs.fetch(:backoff, DEFAULT_BACKOFF)
      @retry_count  = 0
    end

    def around_execute(&block)
      begin
        if @delegate.respond_to?(:around_execute)
          @delegate.around_execute(&block)
        else
          block.call
        end
      rescue => e
        logger.error(e.to_s)
        sleep @backoff
        @retry_count += 1
        unless @retry_count > @retries
          logger.debug("retrying (#{@retry_count}/#{@retries})")
          retry
        else
          logger.debug("max retries (#{@retries}) attempted, bailing")
          OpenStruct.new(:success? => false)
        end
      end
    end
  end
end
