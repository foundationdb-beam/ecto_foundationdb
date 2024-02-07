defmodule Ecto.Adapters.FoundationDB.Sandbox do
  def checkout(repo) do
    case :persistent_term.get({__MODULE__, repo, :tenant}, nil) do
      nil ->
        db = get_or_create_test_db(repo)
        tenant_name = "#{repo}"
        tenant = :erlfdb_util.create_and_open_tenant(db, [:empty], tenant_name)
        :persistent_term.put({__MODULE__, repo, :tenant}, tenant)
        {tenant_name, tenant}

      _ ->
        raise "FoundationDB Sandbox Tenant named #{repo} is already checked out"
    end
  end

  def checkin(repo) do
    db = :persistent_term.get({__MODULE__, repo, :database})
    :erlfdb_util.clear_and_delete_tenant(db, "#{repo}")
    :persistent_term.erase({__MODULE__, repo, :tenant})
  end

  defp get_or_create_test_db(repo) do
    case :persistent_term.get({__MODULE__, repo, :database}, nil) do
      nil ->
        new_db = :erlfdb_util.get_test_db([])
        :persistent_term.put({__MODULE__, repo, :database}, new_db)
        new_db

      already_initted_db ->
        already_initted_db
    end
  end
end
