module TransformExampleGroup
  def self.included(base)
    base.let(:transform) { Object.new.extend(described_class) }
    base.let(:environment) { double }
    base.let(:registry) { Masamune::Schema::Registry.new(environment) }
    base.after do
      registry.clear!
    end
  end
end

RSpec.configure do |config|
  config.include TransformExampleGroup, :type => :action, :file_path => %r{spec/masamune/transform}
end
