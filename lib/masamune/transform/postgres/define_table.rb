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

module Masamune::Transform::Postgres
  class DefineTable 
    def initialize(options = {})
      @target   = options[:target]
      @files    = options[:files] || []
      @section  = options[:section]
    end

    def locals
      { target: target, files: files, helper: self }
    end

    def target
      TargetPresenter.new(@target)
    end

    def files
      Masamune::Schema::Map.convert_files(@files)
    end

    def section
      @section || :all
    end

    def define_types?
      !post_section?
    end

    def define_tables?
      !post_section?
    end

    def define_functions?
      !post_section?
    end

    def define_sequences?
      !post_section?
    end

    def define_primary_keys?
      !pre_section? && !(target.temporary? || target.primary_keys.empty?)
    end

    def define_inheritance?
      return false unless target.inherited?
      return false if pre_section?
      return true if post_section?
      !target.delay_indexes?
    end

    def define_indexes?
      return false if pre_section?
      return true if post_section?
      !target.delay_indexes?
    end

    def define_foreign_keys?
      return false if pre_section?
      return true if post_section?
      !target.delay_foreign_keys?
    end

    def define_unique_constraints?
      return false if pre_section?
      return true if post_section?
      !target.delay_unique_constraints?
    end

    def insert_rows?
      !pre_section? && !post_section?
    end

    def load_files?
      all_section?
    end

    def perform_analyze?
      return false if pre_section?
      return true if post_section?
      files.any? || target.insert_rows.any?
    end

    private

    def all_section?
      section == :all
    end

    def pre_section?
      section == :pre
    end

    def post_section?
      section == :post
    end

    class TargetPresenter < SimpleDelegator
      def inherited?
        type == :fact && inheritance_constraints
      end

      def delay_indexes?
        type == :fact
      end

      def delay_foreign_keys?
        type == :fact
      end

      def delay_unique_constraints?
        type == :fact
      end
    end
  end
end
