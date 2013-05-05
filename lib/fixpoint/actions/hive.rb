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
      args += ['-e', options[:jobflow] ? encode_sql(options[:exec]) : options[:exec]] if options[:exec]
      args += ['-f', options[:file]] if options[:file]
      args
    end

    # force SQL be enclosed in single quotes, terminated with semicolon
    def encode_sql(sql)
      %q{'} + sql.gsub(/\A'|'\z/,'').gsub(/;\z/,'') + %q{;'}
    end
  end
end
