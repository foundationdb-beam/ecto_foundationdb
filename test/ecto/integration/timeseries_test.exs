defmodule Ecto.Integration.TimeSeriesTest do
  use Ecto.Integration.Case, async: false

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Exception.Unsupported

  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Schemas.Event

  import Ecto.Query

  @moduletag :integration
  describe "timeseries index" do
    test "multiple fields in query", context do
      tenant = context[:tenant]

      query =
        from(
          e in Event,
          where:
            e.date >= ^~D[1970-01-01] and e.date < ^~D[2100-01-01] and
              e.user_id == ^"foo" and
              (e.time >= ^~T[00:00:00] and e.time <= ^~T[00:00:00])
        )

      {:ok, _} =
        %Event{date: ~D[2070-01-01], user_id: "bar", time: ~T[00:00:00.000000]}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, %Event{id: event_id}} =
        %Event{date: ~D[2070-01-01], user_id: "foo", time: ~T[00:00:00.000000]}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert_raise Unsupported, ~r/Default Index query mismatch/, fn ->
        TestRepo.all(query, prefix: tenant)
      end

      query =
        from(
          e in Event,
          where:
            e.date == ^~D[2070-01-01] and
              e.user_id == ^"foo" and
              (e.time >= ^~T[00:00:00] and e.time <= ^~T[00:00:00])
        )

      assert [%Event{id: ^event_id}] = TestRepo.all(query, prefix: tenant)

      query =
        from(
          e in Event,
          where: e.date >= ^~D[1970-01-01] and e.date <= ^~D[2100-01-01]
        )

      assert 2 == length(TestRepo.all(query, prefix: tenant))
    end

    test "timeseries consistency", context do
      tenant = context[:tenant]

      # Insert
      {:ok, event = %Event{id: event_id}} =
        %Event{date: ~D[2070-01-01], user_id: "foo", time: ~T[00:00:00.000000]}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, _} =
        %Event{date: ~D[2777-01-01], user_id: "foo", time: ~T[00:00:00.000000]}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      # Because write_primary: false
      nil = TestRepo.get(Event, event.id, prefix: tenant)

      # All
      query =
        from(e in Event,
          where: e.date > ^~D[1970-01-01] and e.date < ^~D[2100-01-01]
        )

      assert [%Event{}] = TestRepo.all(query, prefix: tenant)

      # Update
      assert {1, _} = TestRepo.update_all(query, [set: [data: "foo"]], prefix: tenant)

      assert [%Event{id: ^event_id, data: "foo"}] = TestRepo.all(query, prefix: tenant)

      # Delete
      assert {1, _} = TestRepo.delete_all(query, prefix: tenant)

      assert [] == TestRepo.all(query, prefix: tenant)
    end
  end
end
