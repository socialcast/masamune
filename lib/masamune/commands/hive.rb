require 'masamune/commands/shell'

module Masamune::Commands
  class Hive
    attr_accessor :file, :exec, :output

    def initialize(opts = {})
      self.file   = opts[:file]
      self.exec   = opts[:exec]
      self.output = opts[:output]
      @shell = Masamune::Commands::Shell.new(fail_fast: true, decorator: self)
    end

    def exec=(sql)
      if sql
        @exec = sql
        @exec.gsub!(/\s\s+/, ' ')
        @exec.strip!
      end
    end

    def interactive?
      !(@exec || @file)
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

    def execute(*args, &block)
      Dir.chdir(Masamune.configuration.var_dir) do
        @shell.execute('hive', *command_args)
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

    def command_args
      args = []
      args << Masamune.configuration.command_options[:hive].call
      args << ['-e', @exec] if @exec
      args << ['-f', @file] if @file
      args.flatten
    end

=begin
    # force SQL be enclosed in single quotes, terminated with semicolon
    def encode_sql(sql, quote = false)
      if quote
        out.gsub!(/\A'|'\z/,'') if out =~ /\A'/
        out.gsub!(/;\z/,'')
        out.gsub!("'", %q("'"))
        %q{'} + out + %q{;'}
      else
        out
      end
    end
=end
  end
end
