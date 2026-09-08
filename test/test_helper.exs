# Spikes fork real OS processes: run them on purpose with `mix test --include spike`.
ExUnit.start(exclude: [:spike])
