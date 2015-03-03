#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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
