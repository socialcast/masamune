module Fixpoint::Actions
  module Hive
    include Fixpoint::Actions::Common

    def hive(options)
      args = []
      args += ['-e', options[:exec]] if options[:exec]
      args += ['-f', options[:file]] if options[:file]

      tmpdir = Dir.tmpdir
      Dir.chdir(tmpdir) do
        interactive('hive', args)
      end
    end

    private

=begin
    def hive_stdout_filter(line)
      @echo ||= false
      if line =~/^OK/
        @echo = true
      end

      if @echo
        puts line
      end
    end

    def hive_exec_escape(sql)
      %q{'} + sql[/\A'?(.*?);'\z?/,1] + %q{;'}
    end
=end
  end
end
