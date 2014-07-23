require 'spec_helper'
require 'active_support/core_ext/string/strip'

describe Masamune::Schema::Dimension do
  let(:instance) do
    described_class.new('user', columns: columns)
  end

  describe '#to_psql' do
    subject { instance.to_psql }

    context 'with simple columns' do
      let(:columns) { [Masamune::Schema::Column.new('tenant_id'), Masamune::Schema::Column.new('user_id')] }

      it 'should eq dimension template' do
        is_expected.to eq <<-EOS.strip_heredoc
          CREATE TABLE IF NOT EXISTS user_dimension
          (
            uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            start_at TIMESTAMP DEFAULT TO_TIMESTAMP(0),
            end_at TIMESTAMP,
            version INT DEFAULT 1,
            last_modified_at TIMESTAMP DEFAULT NOW()
          );

          CREATE INDEX user_dimension_start_at_index ON user_dimension (start_at);
          CREATE INDEX user_dimension_end_at_index ON user_dimension (end_at);
        EOS
      end
    end
  end
end
