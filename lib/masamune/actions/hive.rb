module Masamune::Actions
  module Hive
    include Masamune::Actions::Common

    def hive(opts = {})
      if opts[:file]
        Masamune.print("hive with file #{opts[:file]}")
      elsif opts[:exec]
        Masamune.print("hive with '#{opts[:exec].gsub(/\s+/, ' ').strip}'")
      end

      interactive = !(opts[:exec] || opts[:file])

      Dir.chdir(Masamune.configuration.var_dir) do
        if jobflow = Masamune.configuration.jobflow || opts[:jobflow]
          execute(*elastic_mapreduce_ssh(jobflow, 'hive', *hive_args(opts)), :replace => interactive, :fail_fast => true)
        else
          execute('hive', *hive_args(opts), :replace => interactive, :fail_fast => true) do |line, line_no|
            unless opts[:exec]
              Masamune::logger.debug(line)
            end
          end
        end
      end
    end

    private

    def elastic_mapreduce_ssh(jobflow, *args)
      ['elastic-mapreduce', '--jobflow', jobflow, '--ssh', %Q{"#{args.join(' ')}"}]
    end

    def hive_args(options)
      args = []
      args << Masamune.configuration.command_options[:hive].call
      args << ['-e', encode_sql(options[:exec], options[:jobflow])] if options[:exec]
      args << ['-f', options[:file]] if options[:file]
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
