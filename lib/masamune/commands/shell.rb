require 'open3'
require 'readline'
require 'ostruct'

module Masamune::Commands
  class Shell
    def initialize(opts = {})
      @safe       = opts[:safe] || false
      @fail_fast  = opts[:fail_fast] || false
      @input      = opts[:input]
      @decorator  = opts[:decorator]
    end

    def replace(*args)
      Kernel.exec(*args)
    end

    def before_execute
      if @decorator.respond_to?(:before_execute)
        @decorator.before_execute
      end
    end

    def after_execute
      if @decorator.respond_to?(:after_execute)
        @decorator.after_execute
      end
    end

    def execute(*args, &block)
      STDOUT.sync = STDERR.sync = true
      exit_code = OpenStruct.new(:success? => false)

      if Masamune::configuration.dryrun
        Masamune::trace(args)
        return exit_code unless @safe
      end

      Masamune::logger.debug(args)
      before_execute

      stdin, stdout, stderr, wait_th = Open3.popen3(*args)
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
      if @decorator.respond_to?(:handle_stdout)
        @decorator.handle_stdout(line, line_no)
      else
        Masamune::logger.debug(line)
      end
    end

    def handle_stderr(line, line_no)
      if @decorator.respond_to?(:handle_stderr)
        @decorator.handle_stderr(line, line_no)
      else
        Masamune::logger.debug(line)
      end
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
