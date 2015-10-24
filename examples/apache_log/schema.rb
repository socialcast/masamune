schema :hive do
  table :visits do
    column :created_at, type: :timestamp
    column :user_id, type: :integer
    column :ip_address, type: :string
    column :path, type: :string
    column :user_agent, type: :string
  end
end
