require 'open3'
require 'readline'

module Fixpoint::Actions
  module Common
    # TODO curry prompt
    def interactive(prompt, *args, &block)
      Open3.popen3(*args) do |stdin, stdout, stderr, th|
        Thread.new {
          while !stdin.closed? do
            input = Readline.readline('', true).strip
            stdin.puts input
          end
        }

        t_err = Thread.new {
          while !stderr.eof?  do
            handle_stderr(stderr.gets)
          end
        }

        t_out = Thread.new {
          line_no = 0
          while !stdout.eof?  do
            handle_stdout(prompt, stdout, line_no) do |line, line_no|
              if block_given?
                yield line, line_no
              else
                print line
              end
            end
            line_no += 1
          end
        }

        Process::waitpid(th.pid) rescue nil

        t_err.join
        t_out.join
      end
    end

    private

    # TODO ungets prompt, and retry gets
    def handle_stdout(prompt, stdout, line_no, &block)
      if prompt && line = stdout.gets(prompt)
        print line
      elsif line = stdout.gets
        yield line, line_no
      end
    end

    def handle_stderr(line)
      Fixpoint::configuration.logger.debug(line.chomp)
    end
  end
end
