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

module Masamune
  require 'masamune/environment'
  require 'masamune/has_environment'
  require 'masamune/io'
  require 'masamune/template'
  require 'masamune/commands'
  require 'masamune/last_element'
  require 'masamune/actions'
  require 'masamune/helpers'
  require 'masamune/configuration'
  require 'masamune/data_plan'
  require 'masamune/thor'
  require 'masamune/thor_loader'
  require 'masamune/filesystem'
  require 'masamune/cached_filesystem'
  require 'masamune/method_logger'
  require 'masamune/after_initialize_callbacks'
  require 'masamune/schema'
  require 'masamune/transform'
  require 'masamune/topological_hash'

  extend self
  extend Masamune::HasEnvironment
end
