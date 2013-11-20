require 'spec_helper'

describe Masamune::StringFormat do
  let(:instance) { Object.new.extend(described_class) }

  describe '.strip_sql' do
    subject { instance.strip_sql(input) }

    context 'with quoted sql' do
      let(:input) { %q('SELECT * FROM table;') }
      it { should == %q(SELECT * FROM table;) }
    end

    context 'with ; terminated sql' do
      let(:input) { %q(SELECT * FROM table;;) }
      it { should == %q(SELECT * FROM table;) }
    end

    context 'with multi line sql' do
      let(:input) do
        <<-EOS
            SELECT
              *
            FROM
              table
            ;

        EOS
      end
      it { should == %q(SELECT * FROM table;) }
    end

    context 'with un-quoted sql' do
      let(:input) { %q(SELECT * FROM table) }
      it { should == %q(SELECT * FROM table;) }
    end
  end
end
