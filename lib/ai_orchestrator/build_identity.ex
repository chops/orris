defmodule AiOrchestrator.BuildIdentity do
  @moduledoc """
  What this artifact is: the source revision it was built from, the IPC protocol version it
  speaks, and the build identity (toolchain, build environment, application version).

  `phase0/north-star-architecture.org` requires source revision, protocol version and build
  identity to be common and observable in EVERY artifact. Before this module the escript could
  not be asked: `lib/ai_orchestrator/cli.ex` answered `validate`, `run`, `status`, `replay`,
  `list`, `cancel` and `resolve`, and no verb reported identity at all. `ai-orchestrator version`
  is that answer, and `test/contracts/production_escript_test.exs` asks the BUILT ARTIFACT rather
  than asking this module from inside the project's own BEAM.

  Every value is captured at COMPILE time, because an escript carries no repository to ask at run
  time. A moved HEAD recompiles this module: `@external_resource` names the resolved git HEAD file
  and the file its symbolic ref points at, both resolved through `git rev-parse --git-path` so that
  a linked worktree (where `.git` is a file, not a directory) resolves correctly. The production
  artifact is in any case built from a freshly created build path, so the artifact under test is
  never stale.

  When git cannot answer -- an unpacked source tree, or a build with no git on PATH -- the revision
  is the literal `unknown` rather than a guess, and the worktree state is `unknown` rather than
  `clean`. `unknown` is a reportable answer, not a silent one.
  """

  use Boundary, deps: [], exports: []

  # a module-body binding rather than an attribute: it is read only by the compile-time captures
  # below, and an attribute read only from another attribute is unused under --warnings-as-errors
  unknown = "unknown"

  # a compile-time git that answers {:ok, trimmed} or :error, and never raises the build down
  git = fn args ->
    try do
      case System.cmd("git", args, stderr_to_stdout: true) do
        {output, 0} -> {:ok, String.trim(output)}
        {_output, _status} -> :error
      end
    rescue
      _error -> :error
    end
  end

  git_path = fn name ->
    case git.(["rev-parse", "--git-path", name]) do
      {:ok, path} -> [path]
      :error -> []
    end
  end

  head_files =
    git_path.("HEAD") ++
      case git.(["symbolic-ref", "--quiet", "HEAD"]) do
        {:ok, ref} -> git_path.(ref)
        :error -> []
      end

  for path <- head_files, File.regular?(path) do
    @external_resource path
  end

  @source_revision (case git.(["rev-parse", "HEAD"]) do
                      {:ok, value} ->
                        if Regex.match?(~r/\A[0-9a-f]{40}\z/, value), do: value, else: unknown

                      :error ->
                        unknown
                    end)

  # `modified` covers tracked edits and untracked files alike: both mean the bytes built here are
  # not exactly the bytes at the reported revision, which is the only thing a reader can act on.
  @source_worktree (case git.(["status", "--porcelain"]) do
                      {:ok, ""} -> "clean"
                      {:ok, _changes} -> "modified"
                      :error -> unknown
                    end)

  @application to_string(Mix.Project.config()[:app])
  @artifact to_string(Mix.Project.config()[:escript][:name])
  @version to_string(Mix.Project.config()[:version])
  @build_env to_string(Mix.env())
  @build_elixir System.version()

  @build_otp (
               release = List.to_string(:erlang.system_info(:otp_release))
               path = Path.join([List.to_string(:code.root_dir()), "releases", release, "OTP_VERSION"])

               case File.read(path) do
                 {:ok, contents} -> String.trim(contents)
                 {:error, _reason} -> release
               end
             )

  # The IPC protocol version this consumer speaks on the wire. It is not free-standing: the v2
  # fixture set under test/fixtures/contracts/ipc/v2/ declares the same number in every reply, and
  # test/contracts/production_escript_test.exs binds the two together, so bumping one alone fails.
  @ipc_protocol_version 2

  @type report :: %{optional(String.t()) => String.t() | non_neg_integer()}

  @doc "The artifact's identity as data. String keys, so the CLI can render it as JSON unchanged."
  @spec report() :: report()
  def report do
    %{
      "application" => @application,
      "artifact" => @artifact,
      "build_elixir" => @build_elixir,
      "build_env" => @build_env,
      "build_otp" => @build_otp,
      "ipc_protocol_version" => @ipc_protocol_version,
      "product" => "orris",
      "source_revision" => @source_revision,
      "source_worktree" => @source_worktree,
      "version" => @version
    }
  end

  @doc "The same identity as the operator-facing org rendering the other read verbs use."
  @spec render() :: String.t()
  def render do
    identity = report()

    rows =
      identity
      |> Map.keys()
      |> Enum.sort()
      |> Enum.map(fn key -> "- #{key}: #{Map.fetch!(identity, key)}" end)

    Enum.join(["#+title: Build identity", "", "* Build identity" | rows], "\n") <> "\n"
  end
end
