require 'spec_helper'

describe Masamune::Actions::PostgresAdmin do
  include Masamune::Actions::PostgresAdmin

  subject { postgres_admin(action: action, database: 'zombo') }

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
