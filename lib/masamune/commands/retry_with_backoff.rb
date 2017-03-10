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

module Masamune::Commands
  class RetryWithBackoff < SimpleDelegator
    MAX_RETRY_EXIT_STATUS = 8

    def initialize(delegate, attrs = {})
      super delegate
      @delegate = delegate
      @max_retries = attrs.fetch(:max_retries, configuration.max_retries)
      @backoff      = attrs.fetch(:backoff, configuration.backoff)
      @retry_count  = 0
    end

    def around_execute(&block)
      status =
      if @delegate.respond_to?(:around_execute)
        @delegate.around_execute(&block)
      else
        yield
      end

      raise "exited with code: #{status.exitstatus}" unless status.success?
      status
    rescue => e
      logger.error(e.to_s)
      sleep @backoff
      @retry_count += 1
      if @retry_count > @max_retries
        logger.debug("max retries (#{@max_retries}) attempted, bailing")
        OpenStruct.new(success?: false, exitstatus: MAX_RETRY_EXIT_STATUS)
      else
        logger.debug("retrying (#{@retry_count}/#{@max_retries})")
        retry
      end
    end
  end
end
