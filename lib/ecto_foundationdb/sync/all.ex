defmodule EctoFoundationDB.Sync.All do
  @moduledoc """
  ## Defining a custom query

  By default, `Repo.all(Schema, prefix: tenant)` or `Repo.all_by(Schema, by, prefix: tenant)` is used to query the database. You can override this by providing your own
  `Ecto.Query` in the `{label, query, by}` tuple. This changes the query used to retrieve data, but does not change the
  watch itself.

  ### Example

  ```elixir
  query = from u in User, order_by: [desc: u.inserted_at]

  {:ok,
  socket
  |> put_private(:tenant, open_tenant(socket))
  |> Sync.sync_all(:sorted_users, Repo, [{User, :users, [], query}]))}
  ```

  ### `watch_action`

  - `inserts`: Receive signal for each insert or upsert
  - `deletes`: Receive signal for each delete
  - `collection`: Receive signal for each insert, upsert, or delete
  - `updates`: Receive signal for each update (via `Repo.update/*`)
  - `changes`: Receive signal for each insert, upsert, delete, or update
  """

  @enforce_keys [:label, :queryable, :by, :watch_action]
  defstruct @enforce_keys
end
