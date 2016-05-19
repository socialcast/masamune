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

require 'masamune'
require 'thor'
require 'pry'

module Masamune::Tasks
  class ShellThor < Thor
    include Masamune::Thor
    include Masamune::Actions::DataFlow

    # FIXME: need to add an unnecessary namespace until this issue is fixed:
    # https://github.com/wycats/thor/pull/247
    namespace :shell
    skip_lock!

    desc 'shell', 'Launch an interactive shell'
    skip
    method_option :prompt, desc: 'Set shell prompt', default: 'masamune'
    class_option :start, aliases: '-a', desc: 'Start time', default: '1 month ago'
    def shell_exec
      Pry.start self, prompt: proc { options[:prompt] + '> ' }
    end
    default_task :shell_exec
  end
end
