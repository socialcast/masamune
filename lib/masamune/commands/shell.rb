require 'open3'
require 'readline'
require 'ostruct'

module Masamune::Commands
  class Shell
    def initialize(delegate, opts = {})
      @delegate   = delegate
      @safe       = opts[:safe] || false
      @fail_fast  = opts[:fail_fast] || false
      @input      = opts[:input]
    end

    def replace
      Kernel.exec(*command_args)
    end

    def before_execute
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
    end

    def command_args
      if @delegate.respond_to?(:command_args)
        @delegate.command_args
      else
        raise 'no command_args'
      end
    end

    def execute
      Masamune::logger.debug(command_args)

      STDOUT.sync = STDERR.sync = true
      exit_code = OpenStruct.new(:success? => false)

      if Masamune::configuration.dryrun
        Masamune::trace(args)
        return exit_code unless @safe
      end

      before_execute

      stdin, stdout, stderr, wait_th = Open3.popen3(*command_args)
      Thread.new {
        if @input
          while line = @input.gets
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
      exit_code = wait_th.value if wait_th.value
      t_err.join
      t_out.join

      after_execute

      Masamune::logger.debug(exit_code)
      raise "fail_fast" if @fail_fast unless exit_code.success?
      exit_code
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

    private

    def method_missing(meth, *args)
      if @delegate.respond_to?(meth)
        @delegate.send(meth, *args)
      end
    end

    def respond_to?(meth)
      @delegate.respond_to?(meth)
    end

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
