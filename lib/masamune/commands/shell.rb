require 'open3'
require 'readline'
require 'ostruct'

require 'masamune/proxy_delegate'

module Masamune::Commands
  class Shell
    SIGINT_EXIT_STATUS = 130
    PIPE_TIMEOUT = 10

    include Masamune::ProxyDelegate

    attr_accessor :safe, :fail_fast

    def initialize(delegate, opts = {})
      @delegate       = delegate
      self.safe       = opts.fetch(:safe, false)
      self.fail_fast  = opts.fetch(:fail_fast, true)
      @stdout_line_no = 0
      @stderr_line_no = 0
    end

    def replace
      logger.debug('replace: ' + command_args.join(' '))
      before_execute
      around_execute do
        pid = Process.fork
        if pid
          STDIN.close; STDOUT.close; STDERR.close
          Process.waitpid(pid)
          exit
        else
          exec(command_env, *command_args)
        end
      end
    end

    def before_execute
      logger.debug(command_args)

      if configuration.verbose
        trace(command_args)
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
      if configuration.no_op && !safe
        return OpenStruct.new(:success? => true)
      end

      if @delegate.respond_to?(:around_execute)
        @delegate.around_execute(&block)
      else
        block.call
      end
    end

    def command_env
      (@delegate.respond_to?(:command_env) ? @delegate.command_env : {}).merge('TZ' => 'UTC')
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
      status = OpenStruct.new(:success? => false, :exitstatus => 1)

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
      Open3.popen3(command_env, *command_args) do |p_stdin, p_stdout, p_stderr, t_stdin|
        p_stdin.wait_writable(PIPE_TIMEOUT) or raise "IO stdin not ready for write in #{PIPE_TIMEOUT}"

        Thread.new {
          if @delegate.respond_to?(:stdin)
            @delegate.stdin.rewind
            while line = @delegate.stdin.gets
              trace(line.chomp)
              p_stdin.puts line
              p_stdin.flush
            end
          else
            while !p_stdin.closed? do
              input = Readline.readline('', true).strip
              p_stdin.puts input
              p_stdin.flush
            end
          end
          p_stdin.close unless p_stdin.closed?
        }

        t_stderr = Thread.new {
          while !p_stderr.eof?  do
            handle_stderr(p_stderr)
          end
          p_stderr.close unless p_stderr.closed?
        }

        t_stdout = Thread.new {
          while !p_stdout.eof?  do
            handle_stdout(p_stdout)
          end
          p_stdout.close unless p_stdout.closed?
        }

        [t_stderr, t_stdout, t_stdin].compact.each { |t| t.join }
        logger.debug(t_stdin.value)
        t_stdin.value
      end
    end

    def handle_stdout(io)
      line = io.gets.chomp
      return unless line
      if @delegate.respond_to?(:handle_stdout)
        @delegate.handle_stdout(line, @stdout_line_no)
      else
        logger.debug(line)
      end
      @stdout_line_no += 1
    end

    def handle_stderr(io)
      line = io.gets.chomp
      return unless line
      if @delegate.respond_to?(:handle_stderr)
        @delegate.handle_stderr(line, @stderr_line_no)
      else
        logger.debug(line)
      end
      @stderr_line_no += 1
    end

    def handle_failure(status)
      if @delegate.respond_to?(:handle_failure)
        @delegate.handle_failure(status)
      end
      raise "fail_fast: #{command_args.join(' ')}" if fail_fast
    end

    private

    def exit_code(status, code = 1)
      return code unless status
      status.exitstatus
    end
  end
end
