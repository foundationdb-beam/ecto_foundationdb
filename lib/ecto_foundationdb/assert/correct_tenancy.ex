defmodule EctoFoundationDB.Assert.CorrectTenancy do
  @moduledoc false

  alias EctoFoundationDB.Exception.IncorrectTenancy
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Schema
  alias EctoFoundationDB.SingleTenantRepo
  alias EctoFoundationDB.Tenant

  defp single?(repo_config) do
    case Keyword.fetch(repo_config, :tenant_id) do
      {:ok, _} ->
        true

      :error ->
        false
    end
  end

  def assert_by_schema!(repo_config, schema_meta) do
    if single?(repo_config) do
      assert_single_tenancy_by_schema!(repo_config, schema_meta)
    else
      assert_multi_tenancy_by_schema!(schema_meta)
    end
  end

  def assert_by_private_map!(repo, private) do
    # This data structure is intended to match the one created in Ecto.Repo.Supervisor
    repo_config = [repo: repo] ++ repo.config()

    if single?(repo_config) do
      assert_single_tenancy_by_private_map!(repo_config, private)
    else
      assert_multi_tenancy_by_private_map!(private)
    end
  end

  def assert_by_query!(
        repo_config,
        query = %Ecto.Query{prefix: tenant, from: %Ecto.Query.FromExpr{source: {source, schema}}}
      ) do
    meta =
      %{prefix: tenant} =
      assert_by_schema!(repo_config, %{prefix: tenant, source: source, schema: schema})

    {meta, %{query | prefix: tenant}}
  end

  defp assert_single_tenancy_by_schema!(
         repo_config,
         schema_meta = %{source: source, schema: schema, prefix: tenant}
       ) do
    case fetch_matching_single_tenant(repo_config, tenant) do
      {:ok, tenant} ->
        schema_meta
        |> Map.put(:prefix, tenant)
        |> Map.put(:context, Schema.get_context!(source, schema))

      :error ->
        raise IncorrectTenancy, """
        You've configured this Repo to use a default single tenant, but either \
        #{inspect(schema)} is defining its own tenant, or you've provided the \
        `prefix: tenant` option.

        When the Repo is defined to use a single tenant, all calls on that Repo \
        must not have a tenant defined.
        """
    end
  end

  defp assert_multi_tenancy_by_schema!(
         schema_meta = %{source: source, schema: schema, prefix: tenant}
       ) do
    schema_meta = Map.put(schema_meta, :context, Schema.get_context!(source, schema))

    case Tx.safe?(tenant) do
      {false, :missing_tenant} ->
        raise IncorrectTenancy, """
        FoundationDB Adapter is expecting the struct or query for schema \
        #{inspect(schema)} to include a tenant in the prefix metadata, \
        but a nil prefix was provided.

        Call `Ecto.Adapters.FoundationDB.usetenant(struct, tenant)` before inserting.

        Or use the option `prefix: tenant` on the call to your Repo.
        """

      {true, tenant = %Tenant{}} ->
        Map.put(schema_meta, :prefix, tenant)
    end
  end

  defp assert_single_tenancy_by_private_map!(repo_config, private) do
    case fetch_matching_single_tenant(repo_config, Map.get(private, :tenant, nil)) do
      {:ok, tenant} ->
        %{tenant: tenant}

      :error ->
        raise IncorrectTenancy, """
        FoundationDB Adapter is expecting the provided `:private` map \
        to omit a `:tenant` key for this single-tenant Repo, but one was provided.
        """
    end
  end

  defp assert_multi_tenancy_by_private_map!(%{tenant: tenant}), do: %{tenant: tenant}

  defp assert_multi_tenancy_by_private_map!(_) do
    raise IncorrectTenancy, """
    FoundationDB Adapter is expecting the provided `:private` map \
    to include a `:tenant` key, but none was provided.
    """
  end

  defp fetch_matching_single_tenant(repo_config, nil),
    do: {:ok, SingleTenantRepo.get!(repo_config[:repo])}

  defp fetch_matching_single_tenant(repo_config, tenant) do
    case SingleTenantRepo.get!(repo_config[:repo]) do
      ^tenant ->
        {:ok, tenant}

      _ ->
        :error
    end
  end
end
