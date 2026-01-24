defmodule Ecto.Adapters.FoundationDB.Supervisor do
  @moduledoc false
  use Supervisor

  alias EctoFoundationDB.Sandbox.Sandboxer
  alias EctoFoundationDB.SingleTenantRepo
  alias EctoFoundationDB.WatchJanitor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl true
  def init([config]) do
    children =
      case Keyword.fetch(config, :tenant_id) do
        {:ok, _} ->
          [Sandboxer, {SingleTenantRepo, [config]}, WatchJanitor]

        :error ->
          [Sandboxer, WatchJanitor]
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
