defmodule CliTest.FieldAIndex do
  @moduledoc false
  use EctoFoundationDB.Migration

  def change() do
    [create(index(CliTest.Schema, [:field_a]))]
  end
end
