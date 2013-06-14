require 'masamune/commands/shell'

module Masamune::Commands
  class Hive
    attr_accessor :file, :exec, :output, :quote

    def initialize(opts = {})
      self.file       = opts[:file]
      self.quote      = opts.fetch(:quote, false)
      self.exec       = opts[:exec]
      self.output     = opts[:output]
    end

    def exec=(sql)
      if sql
        if quote
          @exec = quote_sql(strip_sql(sql))
        else
          @exec = strip_sql(sql)
        end
      end
    end

    def interactive?
      !(exec || file)
    end

    def command_args
      args = []
      args << 'hive'
      args << Masamune.configuration.hive[:options].to_a
      args << ['-e', @exec] if @exec
      args << ['-f', @file] if @file
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
      if @tmpfile
        @tmpfile.puts(line)
      else
        Masamune::logger.debug(line)
      end
    end

    private

    def strip_sql(sql)
      out = sql.dup
      out.gsub!(/\s\s+/, ' ')
      out.strip!
      out
    end

    # force SQL be enclosed in single quotes, terminated with semicolon
    def quote_sql(sql)
      out = sql.dup
      out.gsub!(/\A'|'\z/,'') if out =~ /\A'/
      out.gsub!(/;\z/,'')
      out.gsub!("'", %q("'"))
      %q{'} + out + %q{;'}
    end
  end
end
