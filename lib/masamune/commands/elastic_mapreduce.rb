require 'masamune/client'
require 'masamune/proxy_delegate'

module Masamune::Commands
  class ElasticMapReduce
    include Masamune::ClientBehavior
    include Masamune::ProxyDelegate

    DEFAULT_ATTRIBUTES =
    {
      :path       => 'elastic-mapreduce',
      :options    => [],
      :extra      => [],
      :jobflow    => nil,
      :input      => nil,
    }

    def initialize(delegate, attrs = {})
      @delegate = delegate
      DEFAULT_ATTRIBUTES.merge(attrs).each do |name, value|
        instance_variable_set("@#{name}", value)
      end
    end

    def interactive?
      if @delegate.respond_to?(:interactive?)
        @delegate.interactive?
      elsif @extra.any?
        true
      else
        @input == nil
      end
    end

    def stdin
      if @delegate.respond_to?(:input)
        @delegate.stdin
      elsif @input
        @stdin ||= StringIO.new(@input)
      end
    end

    def elastic_mapreduce_command
      args = []
      args << @path
      args << @options.map(&:to_a)
      args << ['--jobflow', @jobflow] if @jobflow
      args.flatten
    end

    def ssh_args
      args = []
      args << elastic_mapreduce_command
      args << '--ssh'
      args << 'exit'
      args.flatten
    end

    def ssh_command
      @ssh_command ||= begin
        result = %x{#{ssh_args.join(' ')}}
        result.sub(/ exit\Z/, '').split(' ')
      end
    end

    def command_args
      args = []
      args << (ssh_command? ? ssh_command : elastic_mapreduce_command)
      args << @extra.map(&:to_a)
      args << @delegate.command_args if @delegate.respond_to?(:command_args)
      args.flatten
    end

    def handle_stdout(line, line_no)
      if line_no == 0 && line =~ /\Assh/
        @delegate.handle_stderr(line, line_no) if @delegate.respond_to?(:handle_stderr)
      else
        @delegate.handle_stdout(line, line_no) if @delegate.respond_to?(:handle_stdout)
      end
    end

    private

    def ssh_command?
      @delegate.respond_to?(:command_args) || @input.present?
    end
  end
end
