module Masamune::Actions
  module Hive
    include Masamune::Actions::Common

    def hive(opts = {})
      if opts[:file]
        Masamune.print("hive with file #{opts[:file]}")
      elsif opts[:exec]
        Masamune.print("hive exec '#{opts[:exec].gsub(/\s+/, ' ').strip}' #{'into ' + opts[:output] if opts[:output]}")
      end

      if Masamune.configuration.jobflow
        opts[:jobflow] = Masamune.configuration.jobflow
      end

      interactive = !(opts[:exec] || opts[:file])

      setup(opts)
      Dir.chdir(Masamune.configuration.var_dir) do
        if jobflow = opts[:jobflow]
          execute(*elastic_mapreduce_ssh(jobflow, 'hive', *hive_args(opts)), :replace => interactive, :fail_fast => true) do |line, line_no|
            process(line, line_no)
          end
        else
          execute('hive', *hive_args(opts), :replace => interactive, :fail_fast => true) do |line, line_no|
            process(line, line_no)
          end
        end
      end
      cleanup(opts)
    end

    private

    def setup(opts)
      if opts[:output]
        @file ||= Tempfile.new('masamune')
      end
    end

    def process(line, line_no)
      if @file
        @file.puts(line)
      else
        Masamune::logger.debug(line)
      end
    end

    def cleanup(opts)
      if opts[:output]
        @file.close
        fs.move_file(@file.path, opts[:output])
        @file.unlink
      end
    end

    def elastic_mapreduce_ssh(jobflow, *args)
      ['elastic-mapreduce', '--jobflow', jobflow, '--ssh', %Q{"#{args.join(' ')}"}]
    end

    def hive_args(opts)
      args = []
      args << Masamune.configuration.command_options[:hive].call
      args << ['-e', encode_sql(opts[:exec], opts[:jobflow])] if opts[:exec]
      args << ['-f', opts[:file]] if opts[:file]
      args.flatten
    end

    # force SQL be enclosed in single quotes, terminated with semicolon
    def encode_sql(sql, quote = false)
      out = sql.dup
      out.gsub!(/\s\s+/, ' ')
      out.strip!
      if quote
        %q{'} + out.gsub(/\A'|'\z/,'').gsub(/;\z/,'') + %q{;'}
      else
        out
      end
    end
  end
end
