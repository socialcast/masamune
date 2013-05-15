require 'find'

module Masamune
  module ThorData
    def find_thor_file(filename)
      thor_file = nil
      Find.find(File.expand_path('..', filename)) do |path|
        next unless File.basename(path) == filename
        thor_file = path
        break
      end
      thor_file
    end

    def load_data(filename)
      thor_filename = find_thor_file(filename)
      data = StringIO.new
      File.open(thor_filename) do |f|
        begin
          line = f.gets
        end until line.match(/^__END__$/)
        while line = f.gets
          data << line 
        end
      end
      data.rewind
      data
    end
  end
end
