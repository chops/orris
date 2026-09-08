defmodule AiOrchestrator.CommandsTest do
  use ExUnit.Case, async: true

  alias AiOrchestrator.Commands
  alias AiOrchestrator.Commands.Arguments
  alias AiOrchestrator.Commands.CommandId
  alias AiOrchestrator.Commands.Idempotency
  alias AiOrchestrator.Commands.Policy
  alias AiOrchestrator.Contract.Command
  alias AiOrchestrator.Contract.Moment
  alias AiOrchestrator.Journal.Schemas.RequestedBy
  alias AiOrchestrator.Test.FixedClock

  @command_id "cmd_01J9X3T2QF5G7H8K1N3P"
  @hash_a "sha256:" <> String.duplicate("a", 64)
  @hash_b "sha256:" <> String.duplicate("b", 64)
  @now %Moment{wall_ts: "2026-09-03T20:00:00Z", unix: 1_788_400_000}
  @operator %{"class" => "operator", "id" => "local_operator"}

  defmodule Executor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(command, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:executed, command})
      {:ok, %{accepted: true}}
    end
  end

  defmodule InvalidExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, _opts), do: :not_a_result
  end

  defmodule SensitiveInvalidExecutor do
    @moduledoc false
    @behaviour AiOrchestrator.Commands.Executor

    @impl true
    def execute(_command, _opts), do: {:unexpected, "sensitive-provider-output"}
  end

  defmodule InvalidClock do
    @moduledoc false
    @behaviour AiOrchestrator.Clock

    @impl true
    def wall_ts, do: "invalid"
    @impl true
    def unix_now, do: :invalid
    @impl true
    def monotonic_ms, do: 0
  end

  test "build creates the NS-41 stamp from one command identity" do
    assert {:ok, %Command{} = command} =
             Commands.build(@operator, "start", %{"spec_hash" => @hash_a, "plan_hash" => @hash_b},
               run_id: "run_0001",
               command_id: @command_id,
               now: @now
             )

    assert command.run_id == "run_0001"
    assert command.now == @now

    assert command.requested_by == %{
             "class" => "operator",
             "id" => "local_operator",
             "command_id" => @command_id,
             "verb" => "start",
             "args_hash" => Arguments.hash("start", command.args)
           }

    assert {:ok, _parsed} = RequestedBy.parse(command.requested_by)
  end

  test "default command moment derives both representations from one clock fact" do
    FixedClock.reset()

    assert {:ok, %Command{now: now}} =
             Commands.build(@operator, "cancel", %{"reason" => "operator_cancel"},
               run_id: "run_0001",
               command_id: @command_id,
               clock: FixedClock
             )

    assert {:ok, datetime, 0} = DateTime.from_iso8601(now.wall_ts)
    assert DateTime.to_unix(datetime) == now.unix
    assert now.unix == FixedClock.base_unix()
  end

  test "invoke refuses an unauthorized command before reaching the executor" do
    assert {:error, %{clause: "command_not_authorized"}} =
             Commands.invoke(
               %{"class" => "agent", "id" => "writer", "run_id" => "run_0001", "assignment_id" => "as_0001"},
               "cancel",
               %{"reason" => "stop"},
               run_id: "run_0001",
               command_id: @command_id,
               now: @now,
               executor: Executor,
               executor_opts: [test_pid: self()]
             )

    refute_received {:executed, _command}
  end

  test "invoke sends one authorized command through the execution port" do
    assert {:ok, %{accepted: true}} =
             Commands.invoke(@operator, "cancel", %{"reason" => "operator_cancel"},
               run_id: "run_0001",
               command_id: @command_id,
               now: @now,
               executor: Executor,
               executor_opts: [test_pid: self()]
             )

    assert_received {:executed, %Command{requested_by: %{"verb" => "cancel"}}}
  end

  test "invoke rejects an executor result outside its contract" do
    assert {:error,
            %{
              clause: "invalid_executor_result",
              result_class: "atom",
              digest: "sha256:" <> digest
            }} =
             Commands.invoke(@operator, "cancel", %{"reason" => "operator_cancel"},
               run_id: "run_0001",
               command_id: @command_id,
               now: @now,
               executor: InvalidExecutor
             )

    assert byte_size(digest) == 64
  end

  test "invalid executor diagnostics never reflect returned payloads" do
    assert {:error, rejection} =
             Commands.invoke(@operator, "cancel", %{"reason" => "operator_cancel"},
               run_id: "run_0001",
               command_id: @command_id,
               now: @now,
               executor: SensitiveInvalidExecutor
             )

    assert rejection.result_class == "tuple"
    refute inspect(rejection) =~ "sensitive-provider-output"
  end

  test "policy table matches the ratified actor-class verb matrix" do
    expected = %{
      "operator" => ~w(start resume resolve_attention repair cancel pause update_context ratify_plan),
      "console" => ~w(start resume resolve_attention repair cancel pause update_context ratify_plan),
      "agent" => ~w(propose_plan propose_context_change),
      "system" => ~w(repair)
    }

    assert Policy.verbs() == expected
    assert RequestedBy.verbs() == expected

    for {class, verbs} <- Policy.verbs(), verb <- verbs do
      actor = actor(class)
      assert {:ok, ^actor} = Policy.authorize(actor, verb)
    end
  end

  test "actor variants reject missing, extra, and cross-run scope" do
    assert {:error, %{clause: "command_actor_fields"}} =
             Policy.authorize(Map.put(@operator, "reason", "extra"), "start")

    assert {:error, %{clause: "invalid_command_actor"}} = Policy.authorize(%{"class" => "system"}, "repair")

    assert {:error, %{clause: "actor_run_id_mismatch"}} =
             Commands.build(actor("agent"), "propose_plan", %{"plan_hash" => @hash_a},
               run_id: "run_other",
               command_id: @command_id,
               now: @now
             )
  end

  test "policy rejects every malformed actor and unauthorized class-verb pair" do
    cases = [
      {%{}, "start", "invalid_command_actor"},
      {%{"class" => "root", "id" => "root"}, "start", "unknown_actor_class"},
      {%{"class" => "operator", "id" => ""}, "start", "command_actor_id"},
      {%{"class" => "operator", "id" => "local", "extra" => "x"}, "start", "command_actor_fields"},
      {%{"class" => "system", "id" => "monitor", "reason" => ""}, "repair", "command_actor_value"},
      {%{"class" => "system", "id" => "monitor", "reason" => String.duplicate("x", 4097)}, "repair",
       "command_actor_value"},
      {@operator, "propose_plan", "command_not_authorized"},
      {actor("agent"), "cancel", "command_not_authorized"},
      {actor("system"), "start", "command_not_authorized"}
    ]

    for {actor, verb, clause} <- cases do
      assert {:error, %{clause: ^clause}} = Policy.authorize(actor, verb)
    end
  end

  test "every verb enforces its exact argument document" do
    valid = valid_args()

    assert valid |> Map.keys() |> Enum.sort() == Arguments.fields() |> Map.keys() |> Enum.sort()

    for {verb, args} <- valid do
      assert {:ok, ^args} = Arguments.validate(verb, args)
      assert {:error, %{clause: "command_argument_fields"}} = Arguments.validate(verb, Map.put(args, "extra", "x"))
    end
  end

  test "argument validation pins every rejection and bounds journal-facing values" do
    assert {:error, %{clause: "unknown_command_verb"}} = Arguments.validate("unknown", %{})

    assert {:error, %{clause: "command_argument_fields"}} =
             Arguments.validate("start", %{"spec_hash" => @hash_a})

    assert {:error, %{clause: "command_argument_fields"}} =
             Arguments.validate("cancel", %{"reason" => "stop", "extra" => "x"})

    for value <- ["", 1, <<255>>, String.duplicate("x", 4097)] do
      assert {:error, %{clause: "command_argument_value", field: "reason"}} =
               Arguments.validate("cancel", %{"reason" => value})
    end

    assert {:ok, _args} = Arguments.validate("cancel", %{"reason" => String.duplicate("x", 4096)})

    assert {:error, %{clause: "command_argument_hash"}} =
             Arguments.validate("start", %{"spec_hash" => "not-a-hash", "plan_hash" => @hash_b})

    assert {:error, %{clause: "repair_kind"}} =
             Arguments.validate("repair", %{"kind" => "rewrite_history", "detail_hash" => @hash_a})

    assert {:error, %{clause: "attention_ids"}} =
             Arguments.validate("resolve_attention", %{"attention_ids" => "att_1,,att_2"})

    assert {:error, %{clause: "invalid_command_arguments"}} = Arguments.validate("cancel", :not_a_map)
  end

  test "ARGS-CANON-1 matches the ratified byte vector and is injective" do
    assert Arguments.bytes("cancel", %{"reason" => "operator_cancel"}) ==
             "ARGS-CANON-1\n" <> <<6::32>> <> "cancel" <> <<6::32>> <> "reason" <> <<15::32>> <> "operator_cancel"

    refute Arguments.hash("x", %{"a" => "1\nb=2"}) == Arguments.hash("x", %{"a" => "1", "b" => "2"})
    refute Arguments.hash("x", %{"ab" => "c"}) == Arguments.hash("x", %{"a" => "bc"})
    refute Arguments.hash("start", %{"a" => "b"}) == Arguments.hash("resume", %{"a" => "b"})
  end

  test "generated command ids carry 128 random bits inside the grammar" do
    ids = Enum.map(1..256, fn _index -> CommandId.generate() end)

    assert Enum.uniq(ids) == ids
    assert Enum.all?(ids, &match?({:ok, ^&1}, CommandId.validate(&1)))
    assert Enum.all?(ids, &(String.length(&1) == 36))
  end

  test "command id validation rejects wrong type, length, and alphabet" do
    for value <- [nil, 123, "short", String.duplicate("a", 65), "invalid command id"] do
      assert {:error, %{clause: "invalid_command_id"}} = CommandId.validate(value)
    end
  end

  test "command construction pins every boundary rejection" do
    assert {:error, %{clause: "run_id_required"}} =
             Commands.build(@operator, "cancel", %{"reason" => "stop"}, command_id: @command_id, now: @now)

    assert {:error, %{clause: "invalid_command_id"}} =
             Commands.build(@operator, "cancel", %{"reason" => "stop"},
               run_id: "run_0001",
               command_id: "short",
               now: @now
             )

    assert {:error, %{clause: "invalid_command_moment"}} =
             Commands.build(@operator, "cancel", %{"reason" => "stop"},
               run_id: "run_0001",
               command_id: @command_id,
               now: :not_a_moment
             )

    assert {:error, %{clause: "invalid_command_options"}} =
             Commands.build(@operator, "cancel", %{"reason" => "stop"}, %{})

    assert {:error, %{clause: "invalid_command_verb"}} =
             Commands.build(@operator, :cancel, %{"reason" => "stop"}, [])

    assert {:error, %{clause: "invalid_command_id_generator"}} =
             Commands.build(@operator, "cancel", %{"reason" => "stop"},
               run_id: "run_0001",
               command_id_generator: Enum,
               now: @now
             )

    assert {:error, %{clause: "invalid_command_clock"}} =
             Commands.build(@operator, "cancel", %{"reason" => "stop"},
               run_id: "run_0001",
               command_id: @command_id,
               clock: Enum
             )

    assert {:error, %{clause: "invalid_command_clock"}} =
             Commands.build(@operator, "cancel", %{"reason" => "stop"},
               run_id: "run_0001",
               command_id: @command_id,
               clock: InvalidClock
             )

    assert {:error, %{clause: "command_executor_required"}} =
             Commands.invoke(@operator, "cancel", %{"reason" => "stop"},
               run_id: "run_0001",
               command_id: @command_id,
               now: @now
             )

    assert {:error, %{clause: "invalid_command_executor"}} =
             Commands.invoke(@operator, "cancel", %{"reason" => "stop"},
               run_id: "run_0001",
               command_id: @command_id,
               now: @now,
               executor: Enum
             )
  end

  test "policy and journal schema accept the same actor shapes and verbs" do
    for {class, verbs} <- Policy.verbs(), verb <- verbs do
      actor = actor(class)

      assert {:ok, command} =
               Commands.build(actor, verb, valid_args(verb),
                 run_id: "run_0001",
                 command_id: @command_id,
                 now: @now
               )

      assert {:ok, _stamp} = RequestedBy.parse(command.requested_by)
    end

    bad_id = %{"class" => "agent", "id" => "Writer", "run_id" => "run_0001", "assignment_id" => "as_0001"}
    assert {:error, _rejection} = Policy.authorize(bad_id, "propose_plan")

    bad_id_stamp =
      Map.merge(bad_id, %{
        "command_id" => @command_id,
        "verb" => "propose_plan",
        "args_hash" => Arguments.hash("propose_plan", valid_args("propose_plan"))
      })

    assert {:error, _errors} = RequestedBy.parse(bad_id_stamp)

    extra = Map.put(@operator, "extra", "x")
    assert {:error, _rejection} = Policy.authorize(extra, "start")

    extra_stamp =
      Map.merge(extra, %{
        "command_id" => @command_id,
        "verb" => "start",
        "args_hash" => Arguments.hash("start", valid_args("start"))
      })

    assert {:error, _errors} = RequestedBy.parse(extra_stamp)
  end

  test "idempotency compares actor, verb, and arguments after command-id lookup" do
    {:ok, command} =
      Commands.build(@operator, "cancel", %{"reason" => "operator_cancel"},
        run_id: "run_0001",
        command_id: @command_id,
        now: @now
      )

    assert :match = Idempotency.compare(command.requested_by, command)

    for field <- ~w(class id verb args_hash) do
      accepted = Map.update!(command.requested_by, field, &(&1 <> "_different"))
      assert {:conflict, ^field} = Idempotency.compare(accepted, command)
    end

    assert {:error, %{clause: "command_id_scope_mismatch"}} =
             Idempotency.compare(Map.put(command.requested_by, "command_id", "cmd_different_00000000"), command)

    assert {:error, %{clause: "invalid_accepted_command_stamp"}} =
             Idempotency.compare(Map.delete(command.requested_by, "verb"), command)

    malformed = %{command | requested_by: Map.delete(command.requested_by, "args_hash")}

    assert {:error, %{clause: "invalid_requested_command_stamp"}} =
             Idempotency.compare(command.requested_by, malformed)

    assert {:error, %{clause: "invalid_idempotency_comparison"}} = Idempotency.compare(%{}, :not_a_command)
  end

  test "agent scope fields are attribution, not retry identity" do
    {:ok, command} =
      Commands.build(actor("agent"), "propose_plan", valid_args("propose_plan"),
        run_id: "run_0001",
        command_id: @command_id,
        now: @now
      )

    accepted =
      command.requested_by
      |> Map.put("run_id", "run_other")
      |> Map.put("assignment_id", "as_other")

    assert :match = Idempotency.compare(accepted, command)
  end

  defp valid_args, do: Map.new(Arguments.fields(), fn {verb, _fields} -> {verb, valid_args(verb)} end)

  defp valid_args("start"), do: %{"spec_hash" => @hash_a, "plan_hash" => @hash_b}
  defp valid_args("resume"), do: %{"recovery_reason" => "operator_resume"}
  defp valid_args("resolve_attention"), do: %{"attention_ids" => "att_0001,att_0002"}
  defp valid_args("repair"), do: %{"kind" => "tail_truncate", "detail_hash" => @hash_a}
  defp valid_args("cancel"), do: %{"reason" => "operator_cancel"}
  defp valid_args("pause"), do: %{"reason" => "operator_pause"}
  defp valid_args("update_context"), do: %{"patch_hash" => @hash_a}
  defp valid_args("propose_context_change"), do: %{"patch_hash" => @hash_a}
  defp valid_args("propose_plan"), do: %{"plan_hash" => @hash_a}
  defp valid_args("ratify_plan"), do: %{"plan_hash" => @hash_a}

  defp actor("operator"), do: @operator
  defp actor("console"), do: %{"class" => "console", "id" => "console_1"}

  defp actor("agent") do
    %{"class" => "agent", "id" => "writer", "run_id" => "run_0001", "assignment_id" => "as_0001"}
  end

  defp actor("system"), do: %{"class" => "system", "id" => "runs_monitor", "reason" => "boot_repair"}
end
