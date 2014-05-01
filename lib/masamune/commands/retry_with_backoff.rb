require 'masamune/proxy_delegate'

module Masamune::Commands
  class RetryWithBackoff
    include Masamune::ProxyDelegate

    DEFAULT_RETRIES = 3
    DEFAULT_BACKOFF = 5
    MAX_RETRY_EXIT_STATUS = 8

    def initialize(delegate, attrs = {})
      @delegate     = delegate
      @retries      = attrs.fetch(:retries, DEFAULT_RETRIES)
      @backoff      = attrs.fetch(:backoff, DEFAULT_BACKOFF)
      @retry_count  = 0
    end

    def around_execute(&block)
      begin
        status = if @delegate.respond_to?(:around_execute)
          @delegate.around_execute(&block)
        else
          block.call
        end
        raise "exited with code: #{status.exitstatus}" unless status.success?
        status
      rescue => e
        logger.error(e.to_s)
        sleep @backoff
        @retry_count += 1
        unless @retry_count > @retries
          logger.debug("retrying (#{@retry_count}/#{@retries})")
          retry
        else
          logger.debug("max retries (#{@retries}) attempted, bailing")
          OpenStruct.new(:success? => false, :exitstatus => MAX_RETRY_EXIT_STATUS)
        end
      end
    end
  end
end
