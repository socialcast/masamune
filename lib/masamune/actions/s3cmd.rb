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

require 'masamune/commands/s3cmd'

module Masamune::Actions
  module S3Cmd
    include Masamune::Commands::S3Cmd::ClassMethods

    def s3cmd(*args, &block)
      opts = args.last.is_a?(Hash) ? args.pop : {}
      opts = opts.to_hash.symbolize_keys
      opts[:extra] = Array.wrap(args)
      opts[:block] = block.to_proc if block_given?

      command = Masamune::Commands::S3Cmd.new(environment, opts)
      command = Masamune::Commands::RetryWithBackoff.new(command, configuration.commands.s3cmd.slice(:retries, :backoff).merge(opts))
      command = Masamune::Commands::Shell.new(command, opts)

      command.execute
    end

    def s3_sync(src, dst)
      s3cmd('sync', s3b(src), s3b(dst, dir: true))
    end
  end
end
