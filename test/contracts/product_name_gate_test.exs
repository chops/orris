defmodule AiOrchestrator.Contracts.ProductNameGateTest do
  use ExUnit.Case, async: true

  @gate Path.expand("../../bin/check-product-name", __DIR__)

  setup do
    root = Path.join(System.tmp_dir!(), "product-name-gate-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, root: root}
  end

  test "protocol and schema version terminology is allowed", %{root: root} do
    write!(root, "contract.org", "ai-orchestrator protocol v2\nschema version 2\nenvelope v2 fixture\n")

    assert {"", 0} = System.cmd(@gate, [root], stderr_to_stdout: true)
  end

  test "product, application, module, and executable version branding is rejected", %{root: root} do
    for {name, text} <- [
          {"product", "ai-orchestrator " <> "v2"},
          {"executable", "ai-orchestrator-" <> "v2"},
          {"application", ":ai_orchestrator_" <> "v2"},
          {"module", "AiOrchestrator." <> "V2"},
          {"reversed", "V2" <> " ai-orchestrator"},
          {"joined_module", "AiOrchestrator" <> "V2"},
          {"joined_application", ":ai_orchestrator" <> "v2"},
          {"joined_executable", "ai-orchestrator" <> "v2"},
          {"joined_reversed", "V2" <> "AiOrchestrator"}
        ] do
      path = "#{name}.txt"
      write!(root, path, text <> "\n")

      assert {output, 1} = System.cmd(@gate, [root], stderr_to_stdout: true)
      assert output =~ "forbidden prototype product-version marker"
      assert output =~ path

      File.rm!(Path.join(root, path))
    end
  end

  test "similar text does not widen the ban", %{root: root} do
    write!(root, "allowed.txt", "v20 ai-orchestrator\nai-orchestrator protocol v2\n")

    assert {"", 0} = System.cmd(@gate, [root], stderr_to_stdout: true)
  end

  test "the archived history exemption remains narrow", %{root: root} do
    write!(root, "docs/history.md", "ai-orchestrator " <> "v2\n")
    assert {"", 0} = System.cmd(@gate, [root], stderr_to_stdout: true)

    write!(root, "docs/current.md", "ai-orchestrator " <> "v2\n")
    assert {output, 1} = System.cmd(@gate, [root], stderr_to_stdout: true)
    assert output =~ "docs/current.md"
    refute output =~ "docs/history.md"
  end

  test "a scanner failure is a gate failure", %{root: root} do
    bin = Path.join(root, "fake-bin")
    File.mkdir_p!(bin)
    fake_rg = Path.join(bin, "rg")
    File.write!(fake_rg, "#!/bin/sh\nexit 2\n")
    File.chmod!(fake_rg, 0o755)

    path = bin <> ":" <> System.fetch_env!("PATH")
    assert {output, 2} = System.cmd(@gate, [root], env: [{"PATH", path}], stderr_to_stdout: true)
    assert output =~ "product-name scan failed (rg exit 2)"
  end

  defp write!(root, relative, contents) do
    path = Path.join(root, relative)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, contents)
  end
end
