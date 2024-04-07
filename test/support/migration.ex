defmodule EctoFoundationDB.Integration.Migration.UserIndex do
  @moduledoc false
  alias EctoFoundationDB.Schemas.User
  use EctoFoundationDB.Migration

  def change() do
    [create(index(User, [:name]))]
  end
end

defmodule EctoFoundationDB.Integration.Migration.EventIndex do
  @moduledoc false
  alias EctoFoundationDB.Schemas.Event
  use EctoFoundationDB.Migration

  def change() do
    [
      create(index(Event, [:date, :user_id, :time], options: [mapped?: false]))
    ]
  end
end
