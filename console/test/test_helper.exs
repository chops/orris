# the core runs (its Host monitor answers real host facts); the console is started per row with a disposable config
{:ok, _} = Application.ensure_all_started(:ai_orchestrator)
{:ok, _} = Application.ensure_all_started(:phoenix)
ExUnit.start(assert_receive_timeout: 1_500)
