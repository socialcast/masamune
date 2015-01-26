require 'spec_helper'

describe Masamune::Transform::DefineSchema do
  let(:transform) { Object.new.extend(described_class) }

  let(:environment) { double }

  context 'for postgres schema' do
    let(:registry) { Masamune::Schema::Registry.new(environment) }

    before do
      registry.schema do
        dimension 'user_account_state', type: :mini do
          column 'name', type: :string, unique: true
          column 'description', type: :string
        end

        dimension 'user', type: :four do
          references :user_account_state
          column 'tenant_id', index: true, natural_key: true
          column 'user_id', index: true, natural_key: true
          column 'preferences', type: :key_value, null: true
        end

        file 'user', headers: true do
          column 'tenant_id', type: :integer
          column 'user_id', type: :integer
          column 'user_account_state.name', type: :string
          column 'preferences_now', type: :json
          column 'start_at', type: :timestamp
          column 'source_kind', type: :string
          column 'delta', type: :integer
        end
      end
    end

    describe '#define_schema' do
      subject(:result) { transform.define_schema(registry).to_s }

      it 'should render combined template' do
        is_expected.to eq Masamune::Template.combine \
          Masamune::Transform::Operator.new('define_schema', source: registry),
          transform.define_table(registry.dimensions['user_account_state']),
          transform.define_table(registry.dimensions['user'])
      end
    end
  end

  context 'for hive schema' do
    let(:registry) { Masamune::Schema::Registry.new(environment, :hql) }

    before do
      registry.schema do
        event 'tenant' do
          attribute 'tenant_id', type: :integer, immutable: true
          attribute 'account_state', type: :string
          attribute 'premium_type', type: :string
          attribute 'preferences', type: :json
        end
      end
    end

    describe '#define_schema' do
      subject(:result) { transform.define_schema(registry).to_s }

      it 'should render combined template' do
        is_expected.to eq Masamune::Template.combine \
          Masamune::Transform::Operator.new('define_schema', source: registry),
          transform.define_event_view(registry.events['tenant'])
      end
    end
  end
end
