require 'open3'
require 'readline'
require 'ostruct'

module Fixpoint::Actions
  module Common
    def prompt
    end

    def interactive?
      false
    end

    def execute(*args, &block)
      STDOUT.sync = STDERR.sync = true
      exit_code = OpenStruct.new(:success? => false)

      Fixpoint::logger.debug(args)
      Open3.popen3(*args) do |stdin, stdout, stderr, wait_th|
        Thread.new {
          while !stdin.closed? do
            input = Readline.readline('', true).strip
            stdin.puts input
          end
        }

        t_err = Thread.new {
          while !stderr.eof?  do
            handle_stderr(stderr)
          end
        }

        t_out = Thread.new {
          while !stdout.eof?  do
            handle_stdout(stdout) do |line, line_no|
              if block_given?
                yield line, line_no
              else
                puts line
              end
            end
          end
        }

        exit_code = wait_th.value if wait_th.value
        Process::waitpid(wait_th.pid) rescue nil
        exit_code = wait_th.value if wait_th.value

        t_err.join
        t_out.join
      end
      Fixpoint::logger.debug(exit_code)
      exit_code
    end

    private

    def handle_stdout(stdout, &block)
      @line_no ||= 0

      line_handler = ->(lines) do
        return unless lines
        lines.split("\n").each do |line|
          yield line, @line_no
          @line_no += 1
        end
      end

      if interactive? && line = stdout.gets(prompt)
        pre_prompt_lines, post_prompt_lines = line.split(prompt)
        line_handler.call(pre_prompt_lines)
        print prompt if line =~ /#{prompt}/
        line_handler.call(post_prompt_lines)
      elsif
        line_handler.call(stdout.gets)
      end
    end

    def handle_stderr(stderr, &block)
      line = stderr.gets
      Fixpoint::logger.debug(line.chomp)
    end
  end
end
