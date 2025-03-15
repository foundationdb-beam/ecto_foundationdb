defmodule EctoFoundationDB.CLI do
  @moduledoc """
  This module provides functions that are to be run by an operator, and not
  part of application code.
  """
  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Tenant

  @default_copy_fields_max_rows 500

  def run_concurrently_for_all_tenants!(repo, caller_fun, options \\ []) do
    options = Keyword.merge(repo.config(), options)
    db = FoundationDB.db(repo)

    ids = Tenant.Backend.list(db, options)

    fun = fn id ->
      tenant = Tenant.open(repo, id, options)
      caller_fun.(tenant)
    end

    default_max_concurrency = System.schedulers_online() * 2

    default_options = [
      ordered: false,
      max_concurrency: default_max_concurrency,
      timeout: :infinity
    ]

    async_stream_options = options[:async_stream_options] || []
    async_stream_options = Keyword.merge(default_options, async_stream_options)

    Task.async_stream(ids, fun, async_stream_options)
  end

  def migrate!(repo, options \\ []) do
    case options[:prefix] do
      nil ->
        # Opening the tenant runs the migration without any extra calls
        up_fun = fn _tenant -> :ok end

        run_concurrently_for_all_tenants!(repo, up_fun, options)
        |> Stream.run()

      id when is_binary(id) ->
        # This is the one place were we allow a tenant_id as the prefix value because it
        # doesn't make a whole lot of sense to call migrate on an already open tenant.
        # However, we allow for both.
        _ = Tenant.open(repo, id, options)
        :ok

      tenant ->
        _ = Tenant.open(repo, tenant.id, options)
        :ok
    end
  end

  def copy_field!(repo, schema, from, to, options \\ []),
    do: copy_fields!(repo, schema, [{from, to}], options)

  def copy_fields!(repo, schema, copies, options \\ []) do
    case options[:prefix] do
      nil ->
        run_concurrently_for_all_tenants!(
          repo,
          &copy_fields!(repo, schema, copies, prefix: &1),
          options
        )
        |> Stream.map(fn {:ok, count} -> count end)
        |> Enum.sum()

      _ ->
        tenant = options[:prefix]
        default_options = [max_rows: @default_copy_fields_max_rows]

        override_options = [
          tx_callback: &tx_apply_copies_with_update!(tenant, &1, repo, schema, &2, copies)
        ]

        stream_options =
          Keyword.merge(default_options, options)
          |> Keyword.merge(override_options)

        repo.stream(schema, stream_options)
        |> Enum.count()
    end
  end

  def delete_fields!(repo, schema, deletes, options) do
    # Pretty involved.
    # 1. Make sure all indexes with the field are dropped.
    # 2. Confirm that the field isn't on the schema
    # 3. Delete the field from the item in the db -- accounting for InternalMetadata items
  end

  defp tx_apply_copies_with_update!(tenant, tx, repo, schema, data_object_stream, copies) do
    Stream.filter(
      data_object_stream,
      &tx_obj_apply_copies_with_update!(tenant, tx, repo, schema, &1, copies)
    )
  end

  defp tx_obj_apply_copies_with_update!(tenant, _tx, repo, schema, data_object, copies) do
    changeset =
      Enum.reduce(copies, nil, fn {from, to}, acc ->
        case {data_object[from], data_object[to]} do
          {nil, _} ->
            acc

          {from_val, nil} when is_nil(acc) ->
            # Read-your-writes transaction guarantees that this get is fully
            # retrieved from the transaction state in memory. Alternatively,
            # we could use the `:noop` trick, but I think this is fine.
            [{_pk_field, pk} | _] = data_object
            struct = repo.get!(schema, pk, prefix: tenant)
            Ecto.Changeset.change(struct, %{to => from_val})

          {from_val, nil} when not is_nil(acc) ->
            Ecto.Changeset.change(acc, %{to => from_val})

          _ ->
            acc
        end
      end)

    case changeset do
      nil ->
        false

      _ ->
        repo.update!(changeset, prefix: tenant)
        true
    end
  end
end
