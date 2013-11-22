module ActionExampleGroup
  def self.included(base)
    base.let(:var_dir) { Dir.mktmpdir('masamune') }
    base.before do
      Masamune.filesystem.add_path(:var_dir, var_dir)
    end
  end
end

RSpec.configure do |config|
  config.include ActionExampleGroup, :type => :action, :example_group => {:file_path => %r{spec/masamune/action}}
end
