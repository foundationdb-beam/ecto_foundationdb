defmodule Ecto.Integration.IndexTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User
  #alias EctoFoundationDB.Schemas.Account
  #alias EctoFoundationDB.Schemas.Product
  #alias EctoFoundationDB.Schemas.Global
  alias Ecto.Adapters.FoundationDB
  #alias Ecto.Adapters.FoundationDB.Transaction
  #alias Ecto.Adapters.FoundationDB.Exception.IncorrectTenancy
  #alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  import Ecto.Query

  @moduletag :integration
  describe "user index" do
    test "index lookups", context do
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

      assert [%User{name: "John"}, %User{name: "John"}] =
               from(u in User, where: u.name == ^"John")
               |> TestRepo.all(prefix: tenant)
    end
  end
end
