defmodule Ecto.Integration.MigrationsTest do
  use Ecto.Integration.MigrationsCase, async: false

  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Schemas.User

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.CLI
  alias EctoFoundationDB.Exception.Unsupported

  import Ecto.Query

  describe "CLI.migrate!/1" do
    test "migrates all tenants", context do
      tenant = context[:tenant]

      {:ok, _user1} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, _user2} =
        %User{name: "James"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, _user3} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      query_fun = fn ->
        from(u in User, where: u.name == ^"John")
        |> TestRepo.all(prefix: tenant)
      end

      assert_raise(Unsupported, ~r/FoundationDB Adapter supports either/, query_fun)

      # Ecto.Integration.MigrationsCase skips the migrations on purpose, so now we'll apply them manually.
      :ok = CLI.migrate!(TestRepo)

      assert [%User{name: "John"}, %User{name: "John"}] = query_fun.()
    end
  end
end
