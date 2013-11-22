module MasamuneExampleGroup
  def self.included(base)
    base.before do
      Thor.send(:include, Masamune::ThorMute)
    end
  end
end
