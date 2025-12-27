defmodule Ecto.Adapters.FoundationDB.Supervisor do
  @moduledoc false
  use Supervisor

  alias EctoFoundationDB.Sandbox.Sandboxer
  alias EctoFoundationDB.WatchJanitor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl true
  def init(_init_arg) do
    children = [Sandboxer, WatchJanitor]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
