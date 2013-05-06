module Fixpoint::Actions
  module Hive
    include Fixpoint::Actions::Common

    def prompt
      'hive> '
    end

    def interactive?
      !(options[:exec] || options[:file])
    end

    def hive(options)
      Dir.chdir(Fixpoint.configuration.var_dir) do
        if options[:jobflow]
          execute(*emr_ssh(options[:jobflow], 'hive', *hive_args(options))) do |line, line_no|
            if line =~ /\Assh/ && line_no == 0
              Fixpoint.logger.debug(line)
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

    def emr_ssh(jobflow, *args)
      ['elastic-mapreduce', '--jobflow', jobflow, '--ssh', %Q{"#{args.join(' ')}"}]
    end

    def hive_args(options)
      args = []
      args << Fixpoint.configuration.options[:hive].call
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
