defmodule AiOrchestrator.Contracts.VerifyRgGateTest do
  use ExUnit.Case, async: true

  @capture_rg Path.expand("../../bin/capture-rg", __DIR__)

  test "capture-rg preserves matches and treats no matches as success" do
    assert {"hit", 0} = run_with_rg("printf hit; exit 0")
    assert {"", 0} = run_with_rg("exit 1")
  end

  test "capture-rg fails closed when the scanner fails" do
    assert {output, 2} = run_with_rg("exit 7")
    assert output =~ "test vocabulary scan failed (rg exit 7)"
  end

  test "bin/verify routes every ripgrep scan through the fail-closed wrapper" do
    verify = "../../bin/verify" |> Path.expand(__DIR__) |> File.read!()

    assert length(Regex.scan(~r/bin\/capture-rg/, verify)) == 3
    refute verify =~ ~r/rg .*\|\| true/
  end

  defp run_with_rg(body) do
    root = Path.join(System.tmp_dir!(), "capture_rg_#{System.unique_integer([:positive])}")
    bin = Path.join(root, "bin")
    File.mkdir_p!(bin)
    on_exit(fn -> File.rm_rf!(root) end)

    rg = Path.join(bin, "rg")
    File.write!(rg, "#!/bin/sh\n#{body}\n")
    File.chmod!(rg, 0o755)

    System.cmd(@capture_rg, ["test vocabulary", "pattern", "."],
      env: [{"PATH", bin <> ":/usr/bin:/bin"}],
      stderr_to_stdout: true
    )
  end
end
