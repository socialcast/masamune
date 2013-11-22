require 'spec_helper'

describe Masamune::Actions::PostgresAdmin do
  let(:klass) do
    Class.new do
      include Masamune::ClientBehavior
      include Masamune::Actions::PostgresAdmin
    end
  end

  let(:instance) { klass.new }
  let(:configuration) { {} }

  before do
    instance.stub_chain(:configuration, :postgres).and_return({})
    instance.stub_chain(:configuration, :postgres_admin).and_return(configuration)
  end

  describe '.postgres_admin' do
    subject { instance.postgres_admin(action: action, database: 'zombo') }

    context 'with :action :create' do
      let(:action) { :create }

      before do
        mock_command(/\Acreatedb/, mock_success)
      end

      it { should be_success }
    end

    context 'with :action :drop' do
      let(:action) { :drop }

      before do
        mock_command(/\Adropdb/, mock_success)
      end

      it { should be_success }
    end
  end
end
