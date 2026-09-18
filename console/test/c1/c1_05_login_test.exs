defmodule C1.LoginTest do
  use ExUnit.Case, async: false
  alias C1.Harness
  alias OrrisConsole.SessionStore

  @mods [OrrisConsole.Endpoint, OrrisConsole.SessionStore, OrrisConsole.Credential]

  defp app!(overrides \\ []) do
    Harness.red!(@mods)
    {root, _} = Harness.fixture_root(["a"])
    config = Harness.config(Harness.merged([roots: %{"alpha" => root}], overrides))
    secret = Harness.credential!(config)
    Harness.start_app!(config)
    {config, secret}
  end

  defp post(config, headers, form) when is_map(form), do: post(config, headers, URI.encode_query(form))

  defp post(config, headers, body) do
    Harness.conn(config, :post, "/login", [{"content-type", "application/x-www-form-urlencoded"} | headers], body)
  end

  test "C1-05a the correct credential with a valid CSRF token logs in, redirects to / and sets a fresh session cookie" do
    {config, secret} = app!()
    cookie = Harness.login!(config, secret)
    assert cookie =~ "_orris_console_key="
    home = Harness.conn(config, :get, "/", [{"cookie", cookie}])
    assert home.status == 200
    assert SessionStore.counts(OrrisConsole.SessionStore) == %{sessions: 1, views: 0}
  end

  test "C1-05b missing or wrong CSRF fails even with the correct secret; a wrong secret fails generically" do
    {config, secret} = app!()
    get = Harness.conn(config, :get, "/login")
    hex = Base.encode16(secret, case: :lower)
    o = Harness.origin(config)
    assert post(config, [{"origin", o}, {"cookie", Harness.cookie(get)}], %{"credential" => hex}).status == 403

    assert post(config, [{"origin", o}, {"cookie", Harness.cookie(get)}], %{
             "credential" => hex,
             "_csrf_token" => "forged"
           }).status == 403

    token = Harness.csrf_token(get.resp_body)

    wrong =
      post(config, [{"origin", o}, {"cookie", Harness.cookie(get)}], %{
        "credential" => String.duplicate("00", 32),
        "_csrf_token" => token
      })

    assert wrong.status in [200, 401]
    refute wrong.resp_body =~ hex
    assert wrong.resp_body =~ "not accepted"
    assert SessionStore.counts(OrrisConsole.SessionStore) == %{sessions: 0, views: 0}
  end

  test "C1-05c the global limiter admits 5 attempts, answers 429 at exhaustion (even for the correct secret) and recovers after refill" do
    clock = C1.Clock.start!()
    {config, secret} = app!(clock: C1.Clock.fun(clock))
    get = Harness.conn(config, :get, "/login")
    token = Harness.csrf_token(get.resp_body)
    headers = [{"origin", Harness.origin(config)}, {"cookie", Harness.cookie(get)}]
    hex = Base.encode16(secret, case: :lower)

    for _ <- 1..5,
        do: assert(post(config, headers, %{"credential" => "00", "_csrf_token" => token}).status in [200, 401])

    assert post(config, headers, %{"credential" => hex, "_csrf_token" => token}).status == 429
    C1.Clock.advance(clock, 6_000)
    assert post(config, headers, %{"credential" => hex, "_csrf_token" => token}).status == 302
  end

  test "C1-05d a pre-login cookie cannot fix the authenticated session id: login renews it" do
    {config, secret} = app!()
    pre = Harness.cookie(Harness.conn(config, :get, "/login"))
    cookie = Harness.login!(config, secret)
    assert cookie != pre
    stale = Harness.conn(config, :get, "/", [{"cookie", pre}])
    assert stale.status == 302 and Plug.Conn.get_resp_header(stale, "location") == ["/login"]
  end

  # C1-07a refuses a garbage cookie at the socket upgrade; the HTTP path had no row for the case that matters more,
  # a cookie the server itself minted with ONE BIT changed. The mechanism is the same on both transports
  # (Plug.Session.COOKIE fails verification, get_session/2 answers nil, Plugs.RequireSession drops the cookie and
  # redirects), so this states the delivered property on the second transport rather than closing a new hole.
  test "C1-05g a signed session cookie with one bit flipped is no session on the HTTP path" do
    {config, secret} = app!()
    cookie = Harness.login!(config, secret)
    {_id, value} = Harness.raw_session_id(cookie)
    key = apply(OrrisConsole.Endpoint, :session_options, []) |> Keyword.fetch!(:key)

    # the unflipped cookie is admitted: what follows is the flip, not a cookie that never worked
    assert Harness.conn(config, :get, "/", [{"cookie", cookie}]).status == 200

    for {label, flipped} <- [{"payload", flip(value, -2)}, {"signature", flip(value, -1)}] do
      assert flipped != value and byte_size(flipped) == byte_size(value), label

      {conn, calls} =
        Harness.query_calls(fn -> Harness.conn(config, :get, "/", [{"cookie", key <> "=" <> flipped}]) end)

      assert conn.status == 302, "#{label}: #{conn.status}"
      assert Plug.Conn.get_resp_header(conn, "location") == ["/login"], label
      assert calls == [], "#{label}: Query invoked for an unverifiable cookie"
      # measured: the response sets NO cookie at all, so an unverifiable cookie is neither renewed nor minted
      # into a session; a set-cookie here would be session fixation from bytes the server never signed
      assert Plug.Conn.get_resp_header(conn, "set-cookie") == [], label
    end

    # neither flip allocated or consumed anything, and the session the operator really holds still works
    assert SessionStore.counts(OrrisConsole.SessionStore) == %{sessions: 1, views: 0}
    assert Harness.conn(config, :get, "/", [{"cookie", cookie}]).status == 200
  end

  # Flips the low bit of the first byte of segment `position` (-2 payload, -1 signature) that stays inside the
  # base64url alphabet when flipped, so the cookie is still well formed and it is the SIGNED MATERIAL that changed
  # rather than the cookie grammar. The segment separator is read from the value, not assumed.
  defp flip(value, position) do
    separator = if String.contains?(value, "--"), do: "--", else: "."
    segments = String.split(value, separator)
    segment = Enum.at(segments, position)
    segments |> List.replace_at(position, flip_bit(segment)) |> Enum.join(separator)
  end

  defp flip_bit(segment) do
    candidates = for {byte, index} <- Enum.with_index(:binary.bin_to_list(segment)), flippable?(byte), do: index

    case candidates do
      [index | _] ->
        <<head::binary-size(^index), byte, rest::binary>> = segment
        <<head::binary, Bitwise.bxor(byte, 1), rest::binary>>

      [] ->
        flunk("no byte of #{segment} can be bit-flipped inside the base64url alphabet")
    end
  end

  # an even-valued letter or digit flips to its successor, which is a letter or digit again
  defp flippable?(byte), do: rem(byte, 2) == 0 and (byte in ?A..?Y or byte in ?a..?y or byte in ?0..?8)

  test "C1-05e store bounds: the 129th login is refused as capacity; malformed input allocates nothing" do
    {_config, secret} = app!(login_capacity: 200, login_refill_ms: 1)
    store = OrrisConsole.SessionStore

    ids =
      for _ <- 1..128,
          do:
            (
              assert({:ok, id} = SessionStore.login(store, secret))
              id
            )

    assert length(Enum.uniq(ids)) == 128
    assert {:error, :capacity} = SessionStore.login(store, secret)
    assert SessionStore.counts(store).sessions == 128
    before = :erlang.system_info(:atom_count)

    for junk <- [:crypto.strong_rand_bytes(1_000_000), 42, nil, %{}, String.duplicate("x", 5_000)] do
      assert {:error, :invalid} = SessionStore.login(store, junk)
    end

    assert SessionStore.counts(store).sessions == 128
    assert :erlang.system_info(:atom_count) == before
  end

  test "C1-05f a login body above max_login_body is refused (413) before any credential comparison, and the limiter is not charged" do
    {config, secret} = app!()
    get = Harness.conn(config, :get, "/login")
    token = Harness.csrf_token(get.resp_body)
    headers = [{"origin", Harness.origin(config)}, {"cookie", Harness.cookie(get)}]

    big =
      URI.encode_query(%{
        "credential" => Base.encode16(secret, case: :lower),
        "_csrf_token" => token,
        "pad" => String.duplicate("x", 5_000)
      })

    assert post(config, headers, big).status == 413
    assert SessionStore.counts(OrrisConsole.SessionStore).sessions == 0

    assert post(config, headers, %{"credential" => Base.encode16(secret, case: :lower), "_csrf_token" => token}).status ==
             302
  end
end
