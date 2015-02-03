module TransformExampleGroup
  def self.included(base)
    base.let(:transform) { Object.new.extend(described_class) }
    base.let(:environment) { double }
    base.let(:catalog) { Masamune::Schema::Catalog.new(environment) }
    base.after do
      catalog.clear!
    end
  end
end

RSpec.configure do |config|
  config.include TransformExampleGroup, :type => :action, :file_path => %r{spec/masamune/transform}
end
