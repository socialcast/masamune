require 'open3'
require 'readline'
require 'ostruct'

module Masamune::Commands
  class Shell
    SIGINT_EXIT_STATUS = 130

    require 'masamune/proxy_delegate'
    include Masamune::ProxyDelegate

    attr_accessor :safe, :fail_fast, :input

    def initialize(delegate, opts = {})
      @delegate       = delegate
      self.safe       = opts.fetch(:safe, false)
      self.fail_fast  = opts.fetch(:fail_fast, true)
      self.input      = opts[:input]
      @stdout_line_no = 0
      @stderr_line_no = 0
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
        pid = fork {
          exec(*command_args)
        }
        STDERR.reopen(STDOUT)
        Process.waitpid(pid) if pid
        exit
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

    def command_bin
      command_args.first
    end

    def execute
      status = OpenStruct.new(:success? => false)

      before_execute
      status = around_execute do
        execute_block
      end
      after_execute

      unless status.success?
        handle_failure(exit_code(status))
      end
      status
    rescue Interrupt
      handle_failure(SIGINT_EXIT_STATUS)
    rescue SystemExit
      handle_failure(exit_code(status))
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
          handle_stderr(stderr)
        end
        stderr.close
      }

      t_out = Thread.new {
        while !stdout.eof?  do
          handle_stdout(stdout)
        end
        stdout.close
      }

      t_err.join if t_err
      t_out.join if t_out
      wait_th.join
      Masamune::logger.debug(wait_th.value)
      wait_th.value
    ensure
      t_err.join if t_err
      t_out.join if t_out
    end

    def handle_stdout(io)
      line = io.gets.chomp
      return unless line
      if @delegate.respond_to?(:handle_stdout)
        @delegate.handle_stdout(line, @stdout_line_no)
      else
        Masamune::logger.debug(line)
      end
      @stdout_line_no += 1
    end

    def handle_stderr(io)
      line = io.gets.chomp
      return unless line
      if @delegate.respond_to?(:handle_stderr)
        @delegate.handle_stderr(line, @stderr_line_no)
      else
        Masamune::logger.debug(line)
      end
      @stderr_line_no += 1
    end

    def handle_failure(status)
      if @delegate.respond_to?(:handle_failure)
        @delegate.handle_failure(status)
      end
      raise "fail_fast" if fail_fast
    end

    def proxy_methods
      [:before_execute, :around_execute, :after_execute, :command_args, :handle_stdout, :handle_stderr]
    end

    private

    def exit_code(status, code = 1)
      return code unless status
      status.exitstatus
    end
  end
end
