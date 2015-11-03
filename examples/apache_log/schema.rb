#  The MIT License (MIT)
#
#  Copyright (c) 2014-2015, VMware, Inc. All Rights Reserved.
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

schema :postgres do
  file :users, format: :raw, headers: false, json_encoding: :raw do
    column 'data', type: :json
  end

  dimension :user, type: :one do
    column 'user_id',   type: :integer, natural_key: true
    column 'gender', type: :enum, values: %w(none unknown male female)
    column 'nationality', type: :string
  end

  map from: postgres.users_file, to: postgres.user_dimension do |row|
    {
      user_id:      row[:data][:id],
      gender:       row[:data][:gender],
      nationality:  row[:data][:nationality],
    }
  end

  dimension :user_agent, type: :mini do
    column 'name', type: :string, unique: 'shared', index: 'shared'
    column 'os_name', type: :string, unique: 'shared', index: 'shared', default: 'Unknown'
    column 'device', type: :string, unique: 'shared', index: 'shared', default: 'Unknown'
  end

  fact :visits, partition: 'y%Ym%m', grain: %w(hourly) do
    # TODO add date dimension generator
    references :date, degenerate: true
    references :user
    references :user_agent, insert: true

    measure 'total', type: :integer, aggregate: :sum
  end

  file :visits_hourly, headers: false, format: :tsv do
    column 'date.date_id', type: :integer
    column 'user.user_id', type: :integer
    column 'user_agent.name', type: :string
    column 'user_agent.os_name', type: :string
    column 'user_agent.device', type: :string
    column 'time_key', type: :integer
    column 'total', type: :integer
  end
end
