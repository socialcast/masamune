require 'open3'
require 'readline'
require 'ostruct'

module Masamune::Commands
  class Shell
    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    attr_accessor :safe, :fail_fast, :input

    def initialize(delegate, opts = {})
      @delegate       = delegate
      self.safe       = opts.fetch(:safe, false)
      self.fail_fast  = opts.fetch(:fail_fast, false)
      self.input      = opts[:input]
    end

    def input=(input)
      @input =
      case input
      when nil
        nil
      when IO
        input
      when String
        StringIO.new(input)
      else
        raise 'unknown input type'
      end
    end

    def replace
      Masamune::logger.debug('replace: ' + command_args.join(' '))
      around_execute do
        Kernel.exec(*command_args)
      end
    end

    def before_execute
      Masamune::logger.debug(command_args)

      if Masamune::configuration.verbose
        Masamune::trace(command_args)
      end

      if @delegate.respond_to?(:before_execute)
        @delegate.before_execute
      end
    end

    def after_execute
      if @delegate.respond_to?(:after_execute)
        @delegate.after_execute
      end
    end

    def around_execute(&block)
      if Masamune::configuration.no_op && !safe
        return OpenStruct.new(:success? => true)
      end

      if @delegate.respond_to?(:around_execute)
        @delegate.around_execute(&block)
      else
        block.call
      end
    end

    def command_args
      if @delegate.respond_to?(:command_args)
        @delegate.command_args
      else
        raise 'no command_args'
      end
    end

    def execute
      exit_code = OpenStruct.new(:success? => false)

      before_execute
      exit_code = around_execute do
        execute_block
      end
      after_execute

      raise "fail_fast" if fail_fast unless exit_code.success?
      exit_code
    end

    def execute_block
      STDOUT.sync = STDERR.sync = true
      stdin, stdout, stderr, wait_th = Open3.popen3(*command_args)
      Thread.new {
        if input
          while line = input.gets
            stdin.puts line
          end
          stdin.close
        else
          while !stdin.closed? do
            input = Readline.readline('', true).strip
            stdin.puts input
          end
        end
      }

      t_err = Thread.new {
        while !stderr.eof?  do
          handle_stderr_wrapper(stderr)
        end
      }

      t_out = Thread.new {
        while !stdout.eof?  do
          handle_stdout_wrapper(stdout)
        end
      }

      wait_th.join
      Masamune::logger.debug(wait_th.value)
      wait_th.value
    ensure
      t_err.join
      t_out.join
    end

    def handle_stdout(line, line_no)
      if @delegate.respond_to?(:handle_stdout)
        @delegate.handle_stdout(line, line_no)
      else
        Masamune::logger.debug(line)
      end
    end

    def handle_stderr(line, line_no)
      if @delegate.respond_to?(:handle_stderr)
        @delegate.handle_stderr(line, line_no)
      else
        Masamune::logger.debug(line)
      end
    end

    def proxy_methods
      [:before_execute, :around_execute, :after_execute, :command_args, :handle_stdout, :handle_stderr]
    end

    private

    def handle_stdout_wrapper(stdout)
      @line_no ||= 0
      line = stdout.gets
      handle_stdout(line.chomp, @line_no)
      @line_no += 1
    end

    def handle_stderr_wrapper(stderr)
      @line_no ||= 0
      line = stderr.gets
      handle_stderr(line.chomp, @line_no)
      @line_no += 1
    end
  end
end
