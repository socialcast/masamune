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

require 'spec_helper'

describe Masamune::Transform::DefineEventView do
  before do
    catalog.schema :hive do
      event 'tenant' do
        attribute 'tenant_id', type: :integer, immutable: true
        attribute 'account_state', type: :string
        attribute 'premium_type', type: :string
        attribute 'preferences', type: :json
      end
    end
  end

  let(:target) { catalog.hive.tenant_event }

  context 'with hive event' do
    subject(:result) { transform.define_event_view(target).to_s }

    it 'should render define_event_view template' do
      is_expected.to eq <<-EOS.strip_heredoc
        DROP VIEW IF EXISTS tenant_events;
        CREATE VIEW IF NOT EXISTS tenant_events (
          uuid,
          type,
          tenant_id,
          account_state_now,
          account_state_was,
          premium_type_now,
          premium_type_was,
          preferences_now,
          preferences_was,
          delta,
          created_at,
          y, m, d ,h
        ) PARTITIONED ON (y, m, d, h) AS
        SELECT DISTINCT
          uuid,
          type,
          tenant_id,
          account_state_now,
          account_state_was,
          premium_type_now,
          premium_type_was,
          CONCAT('"', REGEXP_REPLACE(preferences_now, '"', '""'),  '"') AS preferences_now,
          CONCAT('"', REGEXP_REPLACE(preferences_was, '"', '""'),  '"') AS preferences_was,
          IF(type = 'tenant_update', 1, 0) AS delta,
          ctime_iso8601 AS created_at,
          y, m, d ,h
        FROM
          events
        LATERAL VIEW
          json_tuple(events.json, 'tenant_id', 'account_state_now', 'account_state_was', 'premium_type_now', 'premium_type_was', 'preferences_now', 'preferences_was') event_data AS tenant_id, account_state_now, account_state_was, premium_type_now, premium_type_was, preferences_now, preferences_was
        WHERE
          type = 'tenant_create' OR
          type = 'tenant_update' OR
          type = 'tenant_delete'
        ;
      EOS
    end
  end
end
