require 'debugger'

module Fixpoint::Actions
  module Hive
    include Fixpoint::Actions::Common

    def hive(options)
      args = []
      args += ['-e', encode_sql(options[:exec])] if options[:exec]
      args += ['-f', options[:file]] if options[:file]

      prompt = (options[:exec] || options[:file]) ? nil : '> '

      Dir.chdir(Fixpoint.configuration.var_dir) do
        if options[:jobflow]
          interactive(prompt, *emr_ssh(options[:jobflow], 'hive', *args)) do |line, line_no|
            if line =~ /\Assh/ && line_no == 0
              Fixpoint.configuration.logger.debug(line)
            else
              puts line
            end
          end
        else
          interactive(prompt, 'hive', *args)
        end
      end
    end

    private

    def emr_ssh(jobflow, *args)
      ['elastic-mapreduce', '--jobflow', jobflow, '--ssh', %Q{"#{args.join(' ')}"}]
    end

    # force SQL be enclosed in single quotes, terminated with semicolon
    def encode_sql(sql)
      %q{'} + sql.gsub(/\A'|'\z/,'').gsub(/;\z/,'') + %q{;'}
    end
  end
end
