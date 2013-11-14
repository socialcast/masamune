module Masamune
  module StringFormat
    def strip_sql(sql)
      out = sql.dup
      out.gsub!(/\A'|\A"|"\z|'\z/, '')
      out.gsub!(/\s\s+/, ' ')
      out.gsub!(/\s*;+\s*$/,'')
      out.strip!
      out + ';'
    end
  end
end
