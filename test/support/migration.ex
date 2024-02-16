defmodule EctoFoundationDB.Integration.Migration do
  @moduledoc false
  use Ecto.Migration

  def change() do
    create(index(:users, [:name]))

    create(index(:events, [:timestamp], options: [timeseries: true]))
  end
end
