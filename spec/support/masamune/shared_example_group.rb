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

module Masamune::SharedExampleGroup
  extend ActiveSupport::Concern

  def capture_popen(cmd, stdin)
    stdout = StringIO.new
    IO.popen(cmd, 'r+') do |io|
      io.sync = true
      io.write stdin
      io.close_write
      stdout << io.read until io.eof?
    end
    stdout.string
  end

  def capture_output(stdout, stderr)
    tmp_stdout = $stdout
    $stdout = stdout
    tmp_stderr = $stderr
    $stderr = stderr
    yield
  ensure
    $stdout = tmp_stdout
    $stderr = tmp_stderr
  end

  def capture(stdout: StringIO.new, stderr: StringIO.new, enable: true)
    if enable
      capture_output(stdout, stderr) do
        yield
      end
    else
      yield
    end
  end

  def with_delim(data, delim = nil)
    return data unless delim
    case delim
    when Hash
      delim.each do |from, to|
        data.gsub!(from, to)
      end
    when Array
      data.gsub!(*delim)
    end
    data
  end

  def load_example_config!
    filesystem.add_path(:current_dir, self.class.example_current_dir) if self.class.example_current_dir
    environment.configuration.load(self.class.example_default_config) if self.class.example_default_config
  end

  # TODO: iterate over databases
  def clean_example_run!
    if configuration.commands.postgres[:clean]
      postgres_admin(action: :drop, database: configuration.commands.postgres[:database])
      postgres_admin(action: :create, database: configuration.commands.postgres[:database])
      postgres(file: define_schema(catalog, :postgres).to_file, retries: 0)
    end
    filesystem.paths.each do |_, (path, options)|
      filesystem.remove_dir(path) if options[:clean]
    end
  end

  # TODO: encapsulate commands as runners
  def setup_example_input!(fixture)
    fixture.inputs.each do |input|
      if input['file']
        filesystem.write(with_delim(input['data'], input['delim']), input['file'])
      end

      hive(exec: input['hive']) if input['hive']

      postgres(exec: input['psql']) if input['psql']
    end
  end

  def gather_example_output(fixture)
    raise "No outputs defined for #{fixture.file_name}" if fixture.outputs.none?
    fixture.outputs.each do |output|
      output_file = output['file'] || Tempfile.new('masamune').path

      execute_output_command(output, output_file)
      next unless output['data']
      actual_data = File.read(output_file).strip
      expect_data = with_delim(output['data'].strip, output['delim']).strip
      if output['order'] == 'random'
        actual_data = actual_data.split("\n").sort.join("\n")
        expect_data = expect_data.split("\n").sort.join("\n")
      end
      yield [actual_data, output_file, expect_data]
    end
  end

  def execute_output_command(output, output_file)
    if output['hive'] && output['hive'].is_a?(String)
      hive(exec: output['hive'], output: output_file)
    elsif output['table']
      table = eval "catalog.#{output['table']}" # rubocop:disable Lint/Eval
      query = denormalize_table(table, output.slice('columns', 'order', 'except', 'include')).to_s
      # FIXME: define format based on table.store.format
      case table.store.type
      when :postgres
        postgres(exec: query, csv: true, output: output_file)
      when :hive
        hive(exec: query, output: output_file)
      else
        raise "'table' output not supported for #{output['table']}"
      end
    elsif output['hive'] && output['hive'].is_a?(Hash)
      hive(file: example_file(output['hive']['file']), output: output_file)
    elsif output['psql'] && output['psql'].is_a?(String)
      postgres(exec: output['psql'], csv: true, output: output_file)
    elsif output['psql'] && output['psql'].is_a?(Hash)
      postgres(file: example_file(output['psql']['file']), variables: output['psql'].fetch('variables', {}), csv: true, output: output_file)
    end
  end

  def example_fixture(options = {})
    Masamune::JobFixture.load(options.merge(type: self.class.example_type), binding)
  end

  module ClassMethods
    def example_current_dir
      example_file_path_info[:current_dir]
    end

    def example_default_config
      return unless respond_to?(:described_class)
      return unless described_class.respond_to?(:class_options)
      described_class.class_options[:config].try(:default)
    end

    def example_fixture_file(options = {})
      Masamune::JobFixture.file_name(options.merge(path: File.dirname(file_path), type: example_type))
    end

    def example_step
      return unless respond_to?(:description)
      File.join(example_current_dir, description)
    end

    def example_type
      example_file_path_info[:example_type]
    end

    private

    EXAMPLE_FILE_PATH_INFO = %r{(?<current_dir>.*?)/spec/((?<example_name>\w+)_)?(?<example_type>\w+)_spec\.rb\z}
    def example_file_path_info
      return {} unless respond_to?(:file_path)
      @example_file_path_info ||= EXAMPLE_FILE_PATH_INFO.match(file_path) || {}
    end

    def example_name
      example_file_path_info[:example_name]
    end
  end
end
