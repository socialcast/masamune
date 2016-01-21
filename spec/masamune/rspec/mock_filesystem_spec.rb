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

describe Masamune::MockFilesystem do
  let(:instance) { described_class.new }

  describe '#glob' do
    before do
      instance.touch!('/a/b/c/1.txt', '/a/b/c/2.txt', '/a/b/c/3.txt')
    end

    subject(:result) { instance.glob(input) }

    context 'with glob for existing file' do
      let(:input) { '/a/b/c/1.txt' }

      it 'contains single matching file' do
        expect(result).to include('/a/b/c/1.txt')
        expect(result.count).to eq(1)
      end
    end

    context 'with glob for existing files' do
      let(:input) { '/a/b/c/*' }

      it 'contains all matching files' do
        expect(result).to include('/a/b/c/1.txt')
        expect(result).to include('/a/b/c/2.txt')
        expect(result).to include('/a/b/c/3.txt')
        expect(result.count).to eq(3)
      end
    end

    context 'with glob for existing files (recursive)' do
      let(:input) { '/a/b/*' }

      it 'contains all matching files and directory' do
        expect(result).to include('/a/b/c')
        expect(result).to include('/a/b/c/1.txt')
        expect(result).to include('/a/b/c/2.txt')
        expect(result).to include('/a/b/c/3.txt')
        expect(result.count).to eq(4)
      end
    end

    context 'with glob for missing file' do
      let(:input) { '/a/b/c/4.txt' }

      it { expect(result).to be_empty }
    end

    context 'with glob for missing directory' do
      let(:input) { '/a/b/d/*' }

      it { expect(result).to be_empty }
    end
  end

  describe '#glob_stat' do
    before do
      instance.touch!('/a/b/c/1.txt', '/a/b/c/2.txt', '/a/b/c/3.txt')
    end

    subject(:result) { instance.glob_stat(input).map(&:name) }

    context 'with glob for existing file' do
      let(:input) { '/a/b/c/1.txt' }

      it 'contains single matching file' do
        expect(result).to include('/a/b/c/1.txt')
        expect(result.count).to eq(1)
      end
    end

    context 'with glob for existing files' do
      let(:input) { '/a/b/c/*' }

      it 'contains all matching files' do
        expect(result).to include('/a/b/c/1.txt')
        expect(result).to include('/a/b/c/2.txt')
        expect(result).to include('/a/b/c/3.txt')
        expect(result.count).to eq(3)
      end
    end

    context 'with glob for existing files (recursive)' do
      let(:input) { '/a/b/*' }

      it 'contains all matching files and directory' do
        expect(result).to include('/a/b/c')
        expect(result).to include('/a/b/c/1.txt')
        expect(result).to include('/a/b/c/2.txt')
        expect(result).to include('/a/b/c/3.txt')
        expect(result.count).to eq(4)
      end
    end

    context 'with glob for missing file' do
      let(:input) { '/a/b/c/4.txt' }

      it { expect(result).to be_empty }
    end

    context 'with glob for missing directory' do
      let(:input) { '/a/b/d/*' }

      it { expect(result).to be_empty }
    end
  end
end
