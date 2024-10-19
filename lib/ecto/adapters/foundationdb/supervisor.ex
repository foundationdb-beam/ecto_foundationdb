defmodule Ecto.Adapters.FoundationDB.Supervisor do
  @moduledoc false
  use Supervisor

  alias EctoFoundationDB.Sandbox.Sandboxer

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl true
  def init(_init_arg) do
    children = [Sandboxer]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
