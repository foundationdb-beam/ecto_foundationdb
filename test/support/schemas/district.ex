defmodule EctoFoundationDB.Schemas.District do
  @moduledoc """
  A composite-primary-key schema, in the shape TPC-C uses.

  `(d_w_id, d_id)` is a natural compound key. The declaration order decides
  the order of the elements in the FDB key tuple, so all districts of one
  warehouse are contiguous, and a query constraining `d_w_id` alone is
  answerable by one GetRange.
  """
  use Ecto.Schema

  @primary_key false
  schema "districts" do
    field(:d_w_id, :integer, primary_key: true)
    field(:d_id, :integer, primary_key: true)
    field(:d_name, :string)
    field(:d_next_o_id, :integer)
    timestamps()
  end
end
