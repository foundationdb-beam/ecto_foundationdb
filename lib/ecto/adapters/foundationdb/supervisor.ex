defmodule Ecto.Adapters.FoundationDB.Supervisor do
  @moduledoc """
  This is a top-level supervisor for the FoundationDB Adapter under which
  children may be added in the future.
  """
  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = []

    Supervisor.init(children, strategy: :one_for_one)
  end
end
