defmodule EctoFoundationDB.Database do
  @moduledoc "See `Ecto.Adapters.FoundationDB`"
  @type t() :: :erlfdb.database()

  alias EctoFoundationDB.Options

  def open(repo) do
    config = repo.config()
    :erlfdb.open(Options.get(config, :cluster_file))
  end
end
