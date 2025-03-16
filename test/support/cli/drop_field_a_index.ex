defmodule CliTest.DropFieldAIndex do
  @moduledoc false
  use EctoFoundationDB.Migration

  def change() do
    [drop(index(CliTest.Schema, [:field_a]))]
  end
end
