require 'masamune/commands/shell'

module Masamune::Commands
  class Hive
    PROMPT = 'hive>'

    attr_accessor :file, :exec, :input, :output, :print, :block, :rollback

    def initialize(opts = {})
      self.file       = opts[:file]
      self.exec       = opts[:exec]
      self.output     = opts[:output]
      self.print      = opts.fetch(:print, false)
      self.block      = opts[:block]
      self.rollback   = opts[:rollback]
    end

    def exec=(sql = nil)
      return unless sql
      self.input = @exec = strip_sql(sql)
    end

    def stdin
      if input
        @stdin ||= StringIO.new(input)
      end
    end

    def interactive?
      !(exec || file)
    end

    def print?
      self.print
    end

    def command_args
      args = []
      args << Masamune.configuration.hive[:path]
      args << Masamune.configuration.hive[:options].map(&:to_a)
      args << ['-f', file] if file
      args.flatten
    end

    def before_execute
      if file
        Masamune.print("hive with file #{file}")
      end

      if exec
        Masamune.print("hive exec '#{exec}' #{'into ' + output if output}")
      end

      if output
        @tmpfile = Tempfile.new('masamune')
      end
    end

    def around_execute(&block)
      Dir.chdir(Masamune.filesystem.path(:var_dir)) do
        yield
      end
    end

    def after_execute
      if output
        @tmpfile.close
        Masamune.filesystem.move_file(@tmpfile.path, output)
        @tmpfile.unlink
      end
    end

    def handle_stdout(line, line_no)
      if line =~ /\A#{PROMPT}/
        Masamune.logger.debug(line)
      else
        block.call(line) if block

        if @tmpfile
          @tmpfile.puts(line)
        else
          Masamune::print(line) if print?
        end
      end
    end

    def handle_failure(status)
      if rollback
        Masamune::logger.error('rolling back')
        rollback.call
      end
    end

    private

    def strip_sql(sql)
      out = sql.dup
      out.gsub!(/\A'|\A"|"\z|'\z/, '')
      out.gsub!(/\s\s+/, ' ')
      out.gsub!(/\s*;+\s*$/,'')
      out.strip!
      out + ';'
    end
  end
end
