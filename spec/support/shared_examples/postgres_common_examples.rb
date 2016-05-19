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

shared_examples_for Masamune::Commands::PostgresCommon do
  describe '#command_env' do
    subject(:env) do
      instance.command_env
    end

    context 'by default' do
      it { expect(env['PGOPTIONS']).to eq('--client-min-messages=warning') }
    end

    context 'with pgpass_file' do
      let(:configuration) { { pgpass_file: 'pgpass_file' } }

      before do
        allow(File).to receive(:readable?) { true }
      end

      it { expect(env['PGPASSFILE']).to eq('pgpass_file') }
    end

    context 'with pgpass_file that is not readable' do
      let(:configuration) { { pgpass_file: 'pgpass_file' } }

      before do
        allow(File).to receive(:readable?) { false }
      end

      it { expect(env).to_not include 'PGPASSFILE' }
    end
  end
end
