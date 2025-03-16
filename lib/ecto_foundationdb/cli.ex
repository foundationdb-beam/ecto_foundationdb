defmodule EctoFoundationDB.CLI do
  @moduledoc """
  This module provides functions that are to be run by an operator, and not
  part of application code.
  """
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Schema

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Tenant

  @default_copy_fields_max_rows 500
  @default_delete_fields_max_rows 500

  @doc """
  Runs the provided 1-arity function for each tenant in the database concurrently.

  ## Arguments

  - `repo`: The Ecto repository to use.
  - `caller_fun`: A 1-arity function; the argument is the tenant.
  - `options`: Optional keyword list of options.

  ## Options

  - `:async_stream_options`: Options to be passed to `Task.async_stream/3`. Defaults to `ordered: false, max_concurrency: System.schedulers_online() * 2, timeout: :infinity`.
  """
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

  @doc """
  Ensures all tenants in the database are migrated to the latest version.

  ## Arguments

  - `repo`: The repository to use.
  - `options`: Options to pass to the migration function.

  ## Options

  - `:prefix`: If provided, only this tenant will be migrated. If not provided, then all tenants concurrently.

  Also supports options from `run_concurrently_for_all_tenants!/3` and `EctoFoundationDB.Tenant.open/3`.
  """
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

  @doc """
  For each schema item in the database, copies the contents of one field to another.

  ## Arguments

  - `repo`: The repository to use.
  - `schema`: The schema to copy from.
  - `from`: The field to copy from.
  - `to`: The field to copy to.
  - `options`: Options for the operation.

  ## Requirements

  - Both fields must exist on the Schema.

  ## Options

  - `:prefix`: If provided, only this tenant will be processed. If not provided, then all tenants concurrently.
  - `:max_rows`: Maximum number of items to process in a single transaction.

  Also supports options from `run_concurrently_for_all_tenants!/3` and `EctoFoundationDB.Tenant.open/3`.
  """
  def copy_field!(repo, schema, from, to, options \\ []),
    do: copy_fields!(repo, schema, [{from, to}], options)

  @doc """
  For each schema item in the database, copies the contents of a set of fields to another set of fields.

  ## Arguments

  - `repo`: The repository to use.
  - `schema`: The schema to copy from.
  - `copies`: A Keyword list of fields to copy from and to.
  - `options`: Options for the operation.

  ## Requirements

  - All fields must exist on the Schema.

  ## Options

  - `:prefix`: If provided, only this tenant will be processed. If not provided, then all tenants concurrently.
  - `:max_rows`: Maximum number of items to process in a single transaction.

  Also supports options from `run_concurrently_for_all_tenants!/3` and `EctoFoundationDB.Tenant.open/3`.
  """
  def copy_fields!(repo, schema, copies, options \\ []) do
    assert_copy_fields!(schema, copies)

    case options[:prefix] do
      nil ->
        run_concurrently_for_all_tenants!(
          repo,
          &_copy_fields!(repo, schema, copies, Keyword.merge(options, prefix: &1)),
          options
        )
        |> Stream.map(fn {:ok, count} -> count end)
        |> Enum.sum()

      _ ->
        _copy_fields!(repo, schema, copies, options)
    end
  end

  defp _copy_fields!(repo, schema, copies, options) do
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

  @doc """
  For each schema item in the database, deletes the given field.

  ## Arguments

  - `repo`: The repository to use.
  - `schema`: The schema to copy from.
  - `field`: The field to delete.
  - `options`: Options for the operation.

  ## Requirements

  - The field must not exist on the Schema.
  - The field must not be used in any registered metadata (indexes).

  ## Options

  - `:prefix`: If provided, only this tenant will be processed. If not provided, then all tenants concurrently.
  - `:max_rows`: Maximum number of items to process in a single transaction.

  Also supports options from `run_concurrently_for_all_tenants!/3` and `EctoFoundationDB.Tenant.open/3`.
  """
  def delete_field!(repo, schema, field, options \\ []) do
    delete_fields!(repo, schema, [field], options)
  end

  @doc """
  For each schema item in the database, deletes the given fields.

  ## Arguments

  - `repo`: The repository to use.
  - `schema`: The schema to copy from.
  - `fields`: The list of fields to delete.
  - `options`: Options for the operation.

  ## Requirements

  - Each field must not exist on the Schema.
  - Each field must not be used in any registered metadata (indexes).

  ## Options

  - `:prefix`: If provided, only this tenant will be processed. If not provided, then all tenants concurrently.
  - `:max_rows`: Maximum number of items to process in a single transaction.

  Also supports options from `run_concurrently_for_all_tenants!/3` and `EctoFoundationDB.Tenant.open/3`.
  """
  def delete_fields!(repo, schema, deletes, options \\ []) do
    if not (options[:skip_schema_assertions] || false) do
      assert_delete_fields_for_schema!(schema, deletes)
    end

    case options[:prefix] do
      nil ->
        run_concurrently_for_all_tenants!(
          repo,
          &_delete_fields!(repo, schema, deletes, Keyword.merge(options, prefix: &1)),
          options
        )
        |> Stream.map(fn {:ok, count} -> count end)
        |> Enum.sum()

      _ ->
        _delete_fields!(repo, schema, deletes, options)
    end
  end

  defp _delete_fields!(repo, schema, deletes, options) do
    assert_delete_fields_for_tenant!(options[:prefix], schema, deletes)

    tenant = options[:prefix]
    default_options = [max_rows: @default_delete_fields_max_rows]

    adapter_opts = repo.config()

    override_options = [
      tx_callback:
        &tx_apply_deletes_with_update!(tenant, &1, repo, schema, &2, deletes, adapter_opts)
    ]

    stream_options =
      Keyword.merge(default_options, options)
      |> Keyword.merge(override_options)

    repo.stream(schema, stream_options)
    |> Enum.count()
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

          {from_val, from_val} ->
            acc

          {from_val, _} when is_nil(acc) ->
            # Read-your-writes transaction guarantees that this get is fully
            # retrieved from the transaction state in memory. Alternatively,
            # we could use the `:noop` trick, but I think this is fine.
            [{_pk_field, pk} | _] = data_object
            struct = repo.get!(schema, pk, prefix: tenant)
            Ecto.Changeset.change(struct, %{to => from_val})

          {from_val, _} when not is_nil(acc) ->
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

  defp tx_apply_deletes_with_update!(
         tenant,
         tx,
         repo,
         schema,
         data_object_stream,
         deletes,
         adapter_opts
       ) do
    Stream.filter(
      data_object_stream,
      &tx_obj_apply_deletes_with_update!(tenant, tx, repo, schema, &1, deletes, adapter_opts)
    )
  end

  defp tx_obj_apply_deletes_with_update!(
         tenant,
         tx,
         _repo,
         schema,
         data_object,
         deletes,
         adapter_opts
       ) do
    obj_keys = Keyword.keys(data_object)

    needs_update? =
      0 < MapSet.size(MapSet.intersection(MapSet.new(obj_keys), MapSet.new(deletes)))

    if needs_update? do
      [{pk_field, pk} | _] = data_object
      source = Schema.get_source(schema)
      context = Schema.get_context!(source, schema)
      write_primary = Schema.get_option(context, :write_primary)
      kv_codec = Pack.primary_codec(tenant, source, pk)

      # We need to perform the 'get' again for the DecodedKV. With the FDB RYW transaction,
      # it should be retrieved from the transaction's in-memory state without performance penalty.
      future = Future.new(schema)
      future = Tx.async_get(tenant, tx, kv_codec, future)
      decoded_kv = Future.result(future)

      metadata =
        Metadata.tx_with_metadata_cache(tenant, tx, nil, source, fn _, metadata -> metadata end)

      Tx.update_data_object(
        tenant,
        tx,
        schema,
        pk_field,
        {decoded_kv, [clear: deletes]},
        metadata,
        write_primary,
        adapter_opts
      )

      true
    else
      false
    end
  end

  defp assert_copy_fields!(schema, copies) do
    fields = schema.__schema__(:fields)

    Enum.each(copies, fn {from, to} ->
      unless Enum.member?(fields, from) and Enum.member?(fields, to) do
        raise ArgumentError, """
        Invalid field names

        #{inspect(from)} and #{inspect(to)} must both exist on #{inspect(schema)}
        """
      end
    end)
  end

  defp assert_delete_fields_for_schema!(schema, deletes) do
    fields = schema.__schema__(:fields)

    Enum.each(deletes, fn field ->
      if field in fields do
        raise ArgumentError, """
        Invalid field names

        #{field} must not exist on #{inspect(schema)}
        """
      end
    end)
  end

  defp assert_delete_fields_for_tenant!(tenant, schema, deletes) do
    source = Schema.get_source(schema)

    Metadata.transactional(tenant, source, fn _tx, metadata ->
      Enum.each(deletes, fn field ->
        assert_delete_field_for_tenant_metadata!(tenant, metadata, field)
      end)
    end)
  end

  defp assert_delete_field_for_tenant_metadata!(tenant, metadata, field) do
    case Metadata.find_indexes_using_field(metadata, field) do
      [] ->
        :ok

      idxs ->
        raise ArgumentError, """
        Invalid field name

        #{inspect(field)} was found on indexes of tenant #{inspect(tenant.id)}. You must drop these indexes before deleting the field.

        #{inspect(idxs)}
        """
    end
  end
end
