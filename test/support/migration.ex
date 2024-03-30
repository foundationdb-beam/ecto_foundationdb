defmodule EctoFoundationDB.Integration.Migration.UserIndex do
  @moduledoc false
  alias EctoFoundationDB.Schemas.User
  use Ecto.Adapters.FoundationDB.Migration

  def change() do
    [create(index(User, [:name]))]
  end
end

defmodule EctoFoundationDB.Integration.Migration.EventIndex do
  @moduledoc false
  alias EctoFoundationDB.Schemas.Event
  use Ecto.Adapters.FoundationDB.Migration

  def change() do
    [
      create(index(Event, [:date, :user_id, :time], options: [mapped?: false]))
    ]
  end
end

defmodule EctoFoundationDB.Integration.Migration.IndexFromEventIndex do
  @moduledoc false
  alias EctoFoundationDB.Schemas.Event
  use Ecto.Adapters.FoundationDB.Migration

  def change() do
    [
      create(index(Event, [:user_id], options: [from: :events_date_user_id_time_index]))
    ]
  end
end
