module MasamuneExampleGroup
  def self.included(base)
    base.before do
      Thor.send(:include, Masamune::ThorMute)
      Masamune.thor_instance = nil
    end
  end
end
