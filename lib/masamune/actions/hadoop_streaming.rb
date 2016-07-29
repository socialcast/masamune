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

module Masamune::Actions
  module HadoopStreaming
    def hadoop_streaming(opts = {})
      opts = opts.to_hash.symbolize_keys

      command = Masamune::Commands::HadoopStreaming.new(environment, aws_emr_options(opts))
      command = Masamune::Commands::AwsEmr.new(command, opts.except(:extra)) if configuration.commands.aws_emr[:cluster_id]
      command = Masamune::Commands::RetryWithBackoff.new(command, configuration.commands.hadoop_streaming.slice(:retries, :backoff).merge(opts))
      command = Masamune::Commands::Shell.new(command, opts)

      command.execute
    end

    private

    def aws_emr_options(opts = {})
      return opts unless configuration.commands.aws_emr[:cluster_id]
      opts.merge(quote: true, upload: false)
    end
  end
end
