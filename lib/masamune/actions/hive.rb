module Masamune::Actions
  module Hive
    include Masamune::Actions::Common

    def prompt
      'hive> '
    end

    def interactive?
      !(options[:exec] || options[:file])
    end

    def hive(options)
      Dir.chdir(Masamune.configuration.var_dir) do
        if jobflow = Masamune.configuration.jobflow || options[:jobflow]
          execute(*elastic_mapreduce_ssh(jobflow, 'hive', *hive_args(options))) do |line, line_no|
            if line =~ /\Assh/ && line_no == 0
              Masamune.logger.debug(line)
            else
              puts line
            end
          end
        else
          execute('hive', *hive_args(options))
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
      sql.gsub!(/\s\s+/, ' ').strip!
      if quote
        %q{'} + sql.gsub(/\A'|'\z/,'').gsub(/;\z/,'') + %q{;'}
      else
        sql
      end
    end
  end
end
