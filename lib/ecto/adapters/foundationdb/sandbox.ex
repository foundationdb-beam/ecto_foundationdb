defmodule Ecto.Adapters.FoundationDB.Sandbox do
  alias Ecto.Adapters.FoundationDB.Tenant

  def open_db() do
    get_or_create_test_db()
  end

  def checkout(repo) do
    case :persistent_term.get({__MODULE__, repo, :tenant}, nil) do
      nil ->
        db = get_or_create_test_db()
        tenant_id = "#{repo}"
        other_tenant_id = "#{repo}.Other"
        tenant = Tenant.clear_open!(db, tenant_id, repo.config())
        other_tenant = Tenant.clear_open!(db, other_tenant_id, repo.config())
        :persistent_term.put({__MODULE__, repo, :tenant}, tenant)
        [tenant: {tenant_id, tenant}, other_tenant: {other_tenant_id, other_tenant}]

      _ ->
        raise "FoundationDB Sandbox Tenant named #{repo} is already checked out"
    end
  end

  def checkin(repo) do
    db = :persistent_term.get({__MODULE__, :database})
    Tenant.clear_delete!(db, "#{repo}", repo.config())
    Tenant.clear_delete!(db, "#{repo}.Other", repo.config())
    :persistent_term.erase({__MODULE__, repo, :tenant})
  end

  defp get_or_create_test_db() do
    case :persistent_term.get({__MODULE__, :database}, nil) do
      nil ->
        new_db = :erlfdb_util.get_test_db([])
        :persistent_term.put({__MODULE__, :database}, new_db)
        new_db

      already_initted_db ->
        already_initted_db
    end
  end
end
