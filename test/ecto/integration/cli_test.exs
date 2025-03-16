defmodule Ecto.Integration.CliTest do
  alias Ecto.Adapters.FoundationDB
  alias EctoFoundationDB.CLI
  alias EctoFoundationDB.Tenant
  use ExUnit.Case, async: true

  defp app_release(0) do
    schema =
      quote do
        defmodule CliTest.Schema do
          use Ecto.Schema

          @primary_key {:id, :binary_id, autogenerate: true}

          schema "cli_test_schema" do
            field(:field_a, :string)
          end
        end
      end

    migrator =
      quote do
        defmodule CliTest.Migrator do
          use EctoFoundationDB.Migrator
          @impl true
          def migrations() do
            [
              {0, CliTest.FieldAIndex}
            ]
          end
        end
      end

    app_code =
      quote do
        defmodule CliTest.App do
          def get_by_val(tenant, val) do
            CliTest.Repo.get_by!(CliTest.Schema, [field_a: val], prefix: tenant)
          end
        end
      end

    Code.eval_quoted(schema)
    Code.eval_quoted(migrator)
    Code.eval_quoted(app_code)
  end

  defp app_release(1) do
    schema =
      quote do
        defmodule CliTest.Schema do
          use Ecto.Schema

          @primary_key {:id, :binary_id, autogenerate: true}

          schema "cli_test_schema" do
            field(:field_a, :string)
            field(:field_b, :string)
          end
        end
      end

    migrator =
      quote do
        defmodule CliTest.Migrator do
          use EctoFoundationDB.Migrator
          @impl true
          def migrations() do
            [
              {0, CliTest.FieldAIndex},
              {1, CliTest.FieldBIndex}
            ]
          end
        end
      end

    app_code =
      quote do
        defmodule CliTest.App do
          def get_by_val(tenant, val) do
            CliTest.Repo.get_by!(CliTest.Schema, [field_a: val], prefix: tenant)
          end
        end
      end

    Code.eval_quoted(schema)
    Code.eval_quoted(migrator)
    Code.eval_quoted(app_code)
  end

  defp app_release(2) do
    schema =
      quote do
        defmodule CliTest.Schema do
          use Ecto.Schema

          @primary_key {:id, :binary_id, autogenerate: true}

          schema "cli_test_schema" do
            field(:field_b, :string)
          end
        end
      end

    migrator =
      quote do
        defmodule CliTest.Migrator do
          use EctoFoundationDB.Migrator
          @impl true
          def migrations() do
            [
              {0, CliTest.FieldAIndex},
              {1, CliTest.FieldBIndex},
              {2, CliTest.DropFieldAIndex}
            ]
          end
        end
      end

    app_code =
      quote do
        defmodule CliTest.App do
          def get_by_val(tenant, val) do
            CliTest.Repo.get_by!(CliTest.Schema, [field_b: val], prefix: tenant)
          end
        end
      end

    Code.eval_quoted(schema)
    Code.eval_quoted(migrator)
    Code.eval_quoted(app_code)
  end

  setup do
    # -------------------------------------------------------
    # Simulate app first boot
    # -------------------------------------------------------
    app_release(0)

    context = TenantsForCase.setup(CliTest.Repo, log: false)

    on_exit(fn ->
      TenantsForCase.exit(CliTest.Repo, context[:tenant_id], context[:other_tenant_id])
    end)

    context
  end

  test "rename column, vsn 0 => vsn 2", context do
    tenant = context[:tenant]

    foo =
      struct(CliTest.Schema, %{field_a: "foo"})
      |> FoundationDB.usetenant(tenant)
      |> CliTest.Repo.insert!()

    # Confirm schema is active
    assert %{field_a: "foo"} = CliTest.Repo.get!(CliTest.Schema, foo.id, prefix: tenant)
    refute :field_b in Map.keys(foo)

    # Confirm index is active
    foo_id = foo.id

    assert %{id: ^foo_id} = apply(CliTest.App, :get_by_val, [tenant, "foo"])

    Code.put_compiler_option(:ignore_module_conflict, true)

    # -------------------------------------------------------
    # Simulate new app release #1, re-open tenant
    # -------------------------------------------------------
    app_release(1)
    tenant = Tenant.open!(CliTest.Repo, tenant.id, log: false)

    bar =
      struct(CliTest.Schema, %{field_a: "bar", field_b: "bar"})
      |> FoundationDB.usetenant(tenant)
      |> CliTest.Repo.insert!()

    # Confirm new schema is active
    assert %{field_a: "bar", field_b: "bar"} =
             CliTest.Repo.get!(CliTest.Schema, bar.id, prefix: tenant)

    # Confirm new index is active
    bar_id = bar.id
    assert %{id: ^bar_id} = apply(CliTest.App, :get_by_val, [tenant, "bar"])

    assert_raise(ArgumentError, ~r/must both exist/, fn ->
      EctoFoundationDB.CLI.copy_field!(CliTest.Repo, CliTest.Schema, :field_a, :field_c,
        prefix: tenant
      )
    end)

    # Use Cli to copy field_a to field_b
    num_copied =
      EctoFoundationDB.CLI.copy_field!(CliTest.Repo, CliTest.Schema, :field_a, :field_b,
        prefix: tenant
      )

    assert 1 == num_copied

    # Confirm old record was updated in the copy step
    assert %{field_a: "foo", field_b: "foo"} =
             CliTest.Repo.get!(CliTest.Schema, foo.id, prefix: tenant)

    # Confirm old record is reachable via new index
    assert %{id: ^foo_id} = apply(CliTest.App, :get_by_val, [tenant, "foo"])

    # Verify premature deletes are not allowed
    assert_raise(ArgumentError, ~r/must not exist/, fn ->
      CLI.delete_field!(CliTest.Repo, CliTest.Schema, :field_a, prefix: tenant)
    end)

    assert_raise(ArgumentError, ~r/was found on indexes of tenant/, fn ->
      CLI.delete_field!(CliTest.Repo, CliTest.Schema, :field_a,
        skip_schema_assertions: true,
        prefix: tenant
      )
    end)

    # -------------------------------------------------------
    # Simulate new app release #2, re-open tenant
    # -------------------------------------------------------
    app_release(2)
    tenant = Tenant.open!(CliTest.Repo, tenant.id, log: false)

    assert Keyword.has_key?(
             CLI.Internal.read_raw_primary_obj(tenant, CliTest.Schema, foo_id),
             :field_a
           )

    num_updated = CLI.delete_field!(CliTest.Repo, CliTest.Schema, :field_a, prefix: tenant)
    assert 2 == num_updated

    refute Keyword.has_key?(
             CLI.Internal.read_raw_primary_obj(tenant, CliTest.Schema, foo_id),
             :field_a
           )
  end
end
