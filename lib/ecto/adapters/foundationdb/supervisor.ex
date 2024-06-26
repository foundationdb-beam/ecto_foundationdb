defmodule Ecto.Adapters.FoundationDB.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl true
  def init(_init_arg) do
    children = []

    Supervisor.init(children, strategy: :one_for_one)
  end
end
