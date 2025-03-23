defmodule CliTest.FieldAIndex do
  @moduledoc false
  use EctoFoundationDB.Migration

  @impl true
  def change() do
    [create(index(CliTest.Schema, [:field_a]))]
  end
end
