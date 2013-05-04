require 'debugger'

module Fixpoint::Actions
  module Hive
    include Fixpoint::Actions::Common

    def hive(options)
      prompt = (options[:exec] || options[:file]) ? nil : 'hive> '

      Dir.chdir(Fixpoint.configuration.var_dir) do
        if options[:jobflow]
          interactive(prompt, *emr_ssh(options[:jobflow], 'hive', *hive_args(options))) do |line, line_no|
            if line =~ /\Assh/ && line_no == 0
              Fixpoint.logger.debug(line)
            else
              puts line
            end
          end
        else
          interactive(prompt, 'hive', *hive_args(options))
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
