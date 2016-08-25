#  The MIT License (MIT)
#
#  Copyright (c) 2014-2016, VMware, Inc. All Rights Reserved.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

describe Masamune::Actions::PostgresAdmin do
  let(:klass) do
    Class.new do
      include Masamune::HasEnvironment
      include Masamune::Actions::PostgresAdmin
    end
  end

  let(:instance) { klass.new }

  shared_context 'retries and backoff' do
    context 'with retries and backoff configured via postgres_admin command' do
      before do
        allow(instance).to receive_message_chain(:configuration, :commands, :postgres).and_return(retries: 1, backoff: 10)
        allow(instance).to receive_message_chain(:configuration, :commands, :postgres_admin).and_return(retries: 3, backoff: 1)
        expect(Masamune::Commands::RetryWithBackoff).to receive(:new).with(anything, hash_including(retries: 3, backoff: 1)).once.and_call_original
      end

      it { is_expected.to be_success }
    end

    context 'with retries and backoff configured via postgres command' do
      before do
        allow(instance).to receive_message_chain(:configuration, :commands, :postgres).and_return(retries: 1, backoff: 10)
        allow(instance).to receive_message_chain(:configuration, :commands, :postgres_admin).and_return({})
        expect(Masamune::Commands::RetryWithBackoff).to receive(:new).with(anything, hash_including(retries: 1, backoff: 10)).once.and_call_original
      end

      it { is_expected.to be_success }
    end
  end

  describe '.postgres_admin' do
    subject { instance.postgres_admin(action: action, database: 'zombo') }

    context 'with :action :create' do
      let(:action) { :create }

      before do
        mock_command(/\APGOPTIONS=.* createdb/, mock_success)
      end

      it { is_expected.to be_success }

      include_context 'retries and backoff'
    end

    context 'with :action :drop' do
      let(:action) { :drop }

      before do
        mock_command(/\APGOPTIONS=.* dropdb/, mock_success)
      end

      it { is_expected.to be_success }

      include_context 'retries and backoff'
    end
  end
end
