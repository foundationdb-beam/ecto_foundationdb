defmodule CliTest.FieldBIndex do
  @moduledoc false
  use EctoFoundationDB.Migration

  @impl true
  def change() do
    [create(index(CliTest.Schema, [:field_b]))]
  end
end
