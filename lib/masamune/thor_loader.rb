module Masamune
  module ThorLoader
    def self.load_default_tasks
      Masamune::ThorLoader.load_thorfiles(File.expand_path('../../../lib/masamune/tasks', __FILE__))
    end

    def self.load_thorfiles(dir)
      Dir.chdir(dir) do
        thor_files = Dir.glob('**/*.rb').delete_if { |x| not File.file?(x) }
        thor_files.each do |f|
          ::Thor::Util.load_thorfile(f)
        end
      end
    end
  end
end
