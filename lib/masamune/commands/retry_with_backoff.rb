require 'delegate'

module Masamune::Commands
  class RetryWithBackoff < SimpleDelegator
    MAX_RETRY_EXIT_STATUS = 8

    def initialize(delegate, attrs = {})
      super delegate
      @delegate     = delegate
      @retries      = attrs.fetch(:retries, configuration.retries)
      @backoff      = attrs.fetch(:backoff, configuration.backoff)
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
