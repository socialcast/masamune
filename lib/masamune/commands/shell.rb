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

require 'open3'
require 'readline'
require 'ostruct'
require 'delegate'

module Masamune::Commands
  class Shell < SimpleDelegator
    SIGINT_EXIT_STATUS = 130
    PIPE_TIMEOUT = 10

    attr_accessor :safe, :fail_fast

    def initialize(delegate, opts = {})
      super delegate
      @delegate       = delegate
      self.safe       = opts.fetch(:safe, false)
      self.fail_fast  = opts.fetch(:fail_fast, true)
      @stdout_line_no = 0
      @stderr_line_no = 0
    end

    def replace(opts = {})
      logger.debug("replace: #{command_info}")
      before_execute
      around_execute do
        pid = Process.fork
        if pid
          Signal.trap('INT') {} # Ensure SIGINT is handled by child process exec
          if opts.fetch(:detach, true)
            detach do
              Process.waitpid(pid)
            end
          else
            Process.waitpid(pid)
          end
          exit
        else
          exec(command_env, *command_args)
        end
      end
    end

    def before_execute
      trace(command_args) if configuration.verbose

      @delegate.before_execute if @delegate.respond_to?(:before_execute)
    end

    def after_execute
      @delegate.after_execute if @delegate.respond_to?(:after_execute)
    end

    def around_execute(&block)
      return OpenStruct.new(success?: true) if configuration.dry_run && !safe

      if @delegate.respond_to?(:around_execute)
        @delegate.around_execute(&block)
      else
        yield
      end
    end

    def command_env
      (@delegate.respond_to?(:command_env) ? @delegate.command_env : {}).merge('TZ' => 'UTC')
    end

    def command_args
      raise 'no command_args' unless @delegate.respond_to?(:command_args) && @delegate.command_args
      Array.wrap(@delegate.command_args).flatten.compact.map(&:to_s)
    end

    def command_bin
      command_args.first
    end

    def execute
      status = OpenStruct.new(success?: false, exitstatus: 1)

      before_execute
      status = around_execute do
        execute_block
      end
      after_execute

      handle_failure(exit_code(status)) unless status.success?
      status
    rescue Interrupt
      handle_failure(SIGINT_EXIT_STATUS)
    rescue SystemExit
      handle_failure(exit_code(status))
    end

    def execute_block
      logger.debug("execute: #{command_info}")

      Open3.popen3(command_env, *command_args) do |p_stdin, p_stdout, p_stderr, t_stdin|
        p_stdin.wait_writable(PIPE_TIMEOUT) || raise("IO stdin not ready for write in #{PIPE_TIMEOUT}")

        Thread.new do
          if @delegate.respond_to?(:stdin)
            @delegate.stdin.rewind
            until @delegate.stdin.eof?
              line = @delegate.stdin.gets
              trace(line.chomp)
              p_stdin.puts line
              p_stdin.flush
            end
          else
            until p_stdin.closed?
              input = Readline.readline('', true).strip
              p_stdin.puts input
              p_stdin.flush
            end
          end
          p_stdin.close unless p_stdin.closed?
        end

        t_stderr = Thread.new do
          handle_stderr(p_stderr) until p_stderr.eof?
          p_stderr.close unless p_stderr.closed?
        end

        t_stdout = Thread.new do
          handle_stdout(p_stdout) until p_stdout.eof?
          p_stdout.close unless p_stdout.closed?
        end

        [t_stderr, t_stdout, t_stdin].compact.each(&:join)
        logger.debug("status: #{t_stdin.value}")
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
      @delegate.handle_failure(status) if @delegate.respond_to?(:handle_failure)
      raise failure_message(status) if fail_fast
    end

    def failure_message(status)
      if @delegate.respond_to?(:failure_message)
        @delegate.failure_message(status)
      else
        "fail_fast: #{command_args.join(' ')}"
      end
    end

    private

    def exit_code(status, code = 1)
      return code unless status
      status.exitstatus
    end

    def detach
      old_stdin = $stdin.dup
      new_stdin = File.open('/dev/null', 'r')
      $stdin.reopen(new_stdin)
      new_stdin.close

      old_stdout = $stdout.dup
      new_stdout = File.open('/dev/null', 'w+')
      $stdout.reopen(new_stdout)
      new_stdout.close

      old_stderr = $stderr.dup
      new_stderr = File.open('/dev/null', 'w+')
      $stderr.reopen(new_stderr)
      new_stderr.close

      yield
    ensure
      $stdin.reopen(old_stdin)
      old_stdin.close

      $stdout.reopen(old_stdout)
      old_stdout.close

      $stderr.reopen(old_stderr)
      old_stderr.close

      `stty sane -F /dev/tty` if RUBY_PLATFORM =~ /linux/
    end

    def command_info
      (command_env.map { |key, val| "#{key}=#{val}" } + command_args).join(' ')
    end
  end
end
