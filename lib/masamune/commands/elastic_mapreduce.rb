require 'masamune/proxy_delegate'
require 'masamune/actions/execute'

module Masamune::Commands
  class ElasticMapReduce
    include Masamune::ProxyDelegate
    include Masamune::Actions::Execute

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
      DEFAULT_ATTRIBUTES.merge(configuration.elastic_mapreduce).merge(attrs).each do |name, value|
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

    # Use elastic-mapreduce to translate jobflow into raw ssh command
    def ssh_command
      @ssh_command ||= begin
        result = nil
        execute(*ssh_args, fail_fast: true, safe: true) do |line|
          result = line.sub(/ exit\Z/, '').split(' ')
        end
        result
      end
    end

    def command_args
      args = []
      args << (ssh_command? ? ssh_command : elastic_mapreduce_command)
      args << @extra
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
