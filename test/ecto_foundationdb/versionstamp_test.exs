defmodule EctoFoundationDB.VersionstampTest do
  use ExUnit.Case, async: true

  alias EctoFoundationDB.Versionstamp

  describe "to_integer/1 and from_integer/1 roundtrip" do
    test "roundtrips a versionstamp where the id has leading zero bytes" do
      # id=121919289961 is 5 bytes, leaving 3 leading zero bytes when stored as 8 bytes.
      # This triggered a FunctionClauseError in from_integer/1 because encode_unsigned
      # strips leading zeros, producing a 10-byte binary instead of the required 12.
      vs = {:versionstamp, 121_919_289_961, 0, 0}
      int = Versionstamp.to_integer(vs)
      assert Versionstamp.from_integer(int) == vs
    end

    test "roundtrips a versionstamp with a large id (no leading zero bytes)" do
      vs = {:versionstamp, 0xFFFFFFFFFFFFFFFF - 1, 0xFFFE, 42}
      int = Versionstamp.to_integer(vs)
      assert Versionstamp.from_integer(int) == vs
    end

    test "roundtrips a versionstamp with zero id" do
      vs = {:versionstamp, 0, 0, 0}
      int = Versionstamp.to_integer(vs)
      assert Versionstamp.from_integer(int) == vs
    end
  end
end
