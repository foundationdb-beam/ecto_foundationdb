defmodule EctoFoundationDB.Integration.Migration.UserIndex do
  @moduledoc false
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Schemas.User2
  use EctoFoundationDB.Migration

  @impl true
  def change() do
    [create(index(User, [:name])), create(index(User2, [:name]))]
  end
end

defmodule EctoFoundationDB.Integration.Migration.UserSchemaMetadata do
  @moduledoc false
  alias EctoFoundationDB.Schemas.User
  use EctoFoundationDB.Migration

  @impl true
  def change() do
    [
      create(metadata(User)),
      create(metadata(User, [:name]))
    ]
  end
end

defmodule EctoFoundationDB.Integration.Migration.EventIndex do
  @moduledoc false
  alias EctoFoundationDB.Schemas.Event
  use EctoFoundationDB.Migration

  @impl true
  def change() do
    [
      create(metadata(Event)),
      create(index(Event, [:date, :user_id, :time], options: [mapped?: false]))
    ]
  end
end

defmodule EctoFoundationDB.Integration.Migration.QueueItemIndex do
  @moduledoc false
  alias EctoFoundationDB.Schemas.QueueItem
  use EctoFoundationDB.Migration

  @impl true
  def change() do
    [
      create(index(QueueItem, [:author]))
    ]
  end
end
