defmodule CliTest.FieldBIndex do
  @moduledoc false
  use EctoFoundationDB.Migration

  def change() do
    [create(index(CliTest.Schema, [:field_b]))]
  end
end
