class Masamune::Matcher
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
    if matched_rule = @unbound_rule.match(input)
      matched_date(matched_rule).strftime(template)
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

  def matched_date(matched_rule)
    matched_attrs = [:year, :month, :day, :hour].select { |x| matched_rule.names.map(&:to_sym).include?(x) }
    DateTime.new(*matched_attrs.map { |x| matched_rule[x].to_i })
  end
end
