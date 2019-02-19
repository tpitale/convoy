defmodule ConvoyTest do
  use ExUnit.Case
  doctest Convoy

  test "greets the world" do
    assert Convoy.hello() == :world
  end
end
