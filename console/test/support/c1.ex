defmodule C1 do
  @moduledoc "TEST SUPPORT boundary: the C1 witnesses reach the core only through its exports and the console only dynamically."
  # test support reaches every console module (it drives the product); outgoing references are not checked here
  use Boundary, check: [out: false]
end
