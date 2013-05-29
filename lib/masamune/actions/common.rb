require 'open3'
require 'readline'
require 'ostruct'

module Masamune::Actions
  module Common
    def execute(*args, &block)
      opts = args.last.is_a?(Hash) ? args.pop : {safe: false}
      STDOUT.sync = STDERR.sync = true
      exit_code = OpenStruct.new(:success? => false)

      if Masamune::configuration.dryrun
        Masamune::trace(args)
        return exit_code unless opts[:safe]
      end

      Masamune::logger.debug(args)

      Kernel.exec(*args) if opts[:replace]

      stdin, stdout, stderr, wait_th = Open3.popen3(*args)
      Thread.new {
        if opts[:stdin]
          while line = opts[:stdin].gets
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
      }

      t_out = Thread.new {
        while !stdout.eof?  do
          handle_stdout(stdout) do |line, line_no|
            if block_given?
              yield line, line_no
            else
              Masamune::logger.debug(line)
            end
          end
        end
      }

      wait_th.join
      exit_code = wait_th.value if wait_th.value
      t_err.join
      t_out.join

      Masamune::logger.debug(exit_code)
      raise "fail_fast" if opts[:fail_fast] unless exit_code.success?
      exit_code
    end

    private

    def handle_stdout(stdout, &block)
      @line_no ||= 0
      line = stdout.gets
      yield line, @line_no
      @line_no += 1
    end

    def handle_stderr(stderr, &block)
      line = stderr.gets
      Masamune::logger.debug(line.chomp)
    end
  end
end
