class Fixpoint::Matcher
  def initialize(rule)
    @rule = rule
    @unbound_rule = unbind_rule(rule)
  end

  def matches?(input)
    @unbound_rule.match(input) != nil
  end

  def bind_date(date)
    date.strftime(@rule)
  end

  def bind(input, template)
    free_template = free_template(template)
    if matched_rule = @unbound_rule.match(input)
      free_template % Hash[matched_rule.names.map(&:to_sym).map { |x| [x, matched_rule[x]]}]
    end
  end

  private

  def unbind_rule(string)
    regexp = string.dup
    regexp.gsub!('%Y', '(?<year>\d{4})')
    regexp.gsub!('%m', '(?<month>\d{2})')
    regexp.gsub!('%d', '(?<day>\d{2})')
    regexp.gsub!('%H', '(?<hour>\d{2})')
    Regexp.compile(regexp)
  end

  def free_template(string)
    template = string.dup
    template.gsub!('%Y', '%{year}')
    template.gsub!('%m', '%{month}')
    template.gsub!('%d', '%{day}')
    template.gsub!('%H', '%{hour}')
    template
  end
end
