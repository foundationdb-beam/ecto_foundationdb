defmodule EctoFoundationDB.VersionstampTest do
  use ExUnit.Case, async: true

  alias EctoFoundationDB.Exception.Unsupported
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

    test "roundtrips a versionstamp with max id" do
      vs = {:versionstamp, 0xFFFFFFFFFFFFFFFF, 0xFFFF, 0xFFFF}
      int = Versionstamp.to_integer(vs)
      assert Versionstamp.from_integer(int) == vs
    end

    test "an incomplete versionstamp raises" do
      vs = {:versionstamp, 0xFFFFFFFFFFFFFFFF, 0xFFFF, 42}

      assert_raise Unsupported,
                   ~r/Versionstamps must be completed before they are useful, so we disallow converting an incomplete versionstamp to an integer/,
                   fn -> Versionstamp.to_integer(vs) end
    end
  end

  describe "min/0 and max/0 sentinels" do
    test "min() is less than any real versionstamp" do
      real_vs = {:versionstamp, 1, 0, 0}
      assert Versionstamp.to_integer(Versionstamp.min()) < Versionstamp.to_integer(real_vs)
    end

    test "max() is greater than any real versionstamp" do
      # A real versionstamp has an 8-byte transaction id; the largest plausible
      # value is well below the all-ones sentinel used by max().
      large_vs = {:versionstamp, 0xFFFFFFFFFFFFFFFF - 1, 0xFFFE, 0xFFFE}
      assert Versionstamp.to_integer(Versionstamp.max()) > Versionstamp.to_integer(large_vs)
    end

    test "max() is not considered incomplete" do
      refute Versionstamp.incomplete?(Versionstamp.max())
    end

    test "incomplete() with any user value is considered incomplete" do
      assert Versionstamp.incomplete?(Versionstamp.incomplete(0))
      assert Versionstamp.incomplete?(Versionstamp.incomplete(42))
    end
  end
end
