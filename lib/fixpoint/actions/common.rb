require 'open3'
require 'readline'

module Fixpoint::Actions
  module Common
    def interactive(command, args = [])
      Open3.popen3(command, *args) do |i, o, e, th|
        Thread.new {
          while !i.closed? do
            input = Readline.readline('', true).strip
            i.puts input
          end
        }

        t_err = Thread.new {
          while !e.eof?  do
            handle_stderr(e.readline)
          end
        }

        t_out = Thread.new {
          while !o.eof?  do
            putc o.readchar
          end
        }

        Process::waitpid(th.pid) rescue nil

        t_err.join
        t_out.join
      end
    end

    private

    def handle_stderr(line)
      Fixpoint::configuration.logger.debug(line.chomp)
    end
  end
end
