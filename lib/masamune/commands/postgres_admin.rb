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
  class PostgresAdmin < SimpleDelegator
    include Masamune::Commands::PostgresCommon

    DEFAULT_ATTRIBUTES =
    {
      :create_db_path => 'createdb',
      :drop_db_path   => 'dropdb',
      :options        => [],
      :hostname       => 'localhost',
      :username       => 'postgres',
      :pgpass_file    => nil,
      :action         => nil,
      :database       => nil
    }

    def initialize(delegate, attrs = {})
      super delegate
      DEFAULT_ATTRIBUTES.merge(configuration.postgres).merge(configuration.postgres_admin).merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    def command_args
      raise ArgumentError, ':database must be given' unless @database
      args = []
      args << command_path
      args << '--host=%s' % @hostname if @hostname
      args << '--username=%s' % @username if @username
      args << '--no-password'
      args << @database
      args.flatten.compact
    end

    private

    def command_path
      case @action
      when :create
        [@create_db_path]
      when :drop
        [@drop_db_path, '--if-exists']
      else
        raise ArgumentError, ':action must be :create or :drop'
      end
    end
  end
end
