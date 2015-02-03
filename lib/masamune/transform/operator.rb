module Masamune::Transform
  class Operator
    def initialize(*args)
      options     = args.last.is_a?(Hash) ? args.pop : {}
      @templates  = args
      @source     = options.delete(:source)
      @target     = options.delete(:target)
      @presenters = options.delete(:presenters) || {}
      @locals     = options
    end

    def source
      return unless @source
      @presenters.key?(source_store.try(:type)) ? @presenters[source_store.try(:type)].new(@source) : @source
    end

    def target
      return unless @target
      @presenters.key?(target_store.try(:type)) ? @presenters[target_store.try(:type)].new(@target) : @target
    end

    def to_s
      result = []
      @templates.each do |template|
        case template
        when Operator
          result << template
        when Symbol, String
          result << template_eval(template)
        end
      end
      Masamune::Template.combine(*result)
    end

    def to_file
      Tempfile.new('masamune').tap do |file|
        file.write(to_s)
        file.close
      end.path
    end

    private

    def source_store
      return @source if @source.is_a?(Masamune::Schema::Store)
      @source.try(:store)
    end

    def target_store
      return @target if @target.is_a?(Masamune::Schema::Store)
      @target.try(:store)
    end

    def template_eval(template)
      return File.read(template) if File.exists?(template.to_s)
      Masamune::Template.render_to_string(template_file(template), @locals.merge(source: source, target: target))
    end

    def template_file(template_prefix)
      File.expand_path(File.join(__FILE__, '..', "#{template_prefix}.#{template_suffix}.erb"))
    end

    def template_suffix
      case (target_store || source_store).try(:type)
      when :postgres
        'psql'
      when :hive
        'hql'
      else
        'txt'
      end
    end
  end
end
