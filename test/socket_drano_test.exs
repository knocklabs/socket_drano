defmodule SocketDranoTest do
  use ExUnit.Case
  doctest SocketDrano

  test "it starts" do
    spec = %{
      id: SocketDrano,
      start: {
        SocketDrano,
        :start_link,
        [
          [
            {:strategy, {:percentage, 25, 100}},
            {:name, SocketDrano},
            {:drain_check_interval, 1000},
            {:shutdown_delay, 5000},
            {:resume_after_drain, false},
            {:refs, [MyApp.Endpoint.HTTP]}
          ]
        ]
      },
      type: :worker
    }

    assert SocketDrano.child_spec(refs: [MyApp.Endpoint.HTTP]) == spec
  end

  test "it takes opts" do
    spec2 = %{
      id: SocketDrano,
      start: {
        SocketDrano,
        :start_link,
        [
          [
            {:name, SocketDrano},
            {:resume_after_drain, false},
            {:strategy, {:percentage, 5, 100}},
            {:drain_check_interval, 500},
            {:shutdown_delay, 10_000},
            {:refs, [MyApp.Endpoint.HTTP, MyApp.Endpoint.HTTPS]}
          ]
        ]
      },
      type: :worker
    }

    assert SocketDrano.child_spec(
             strategy: {:percentage, 5, 100},
             drain_check_interval: 500,
             shutdown_delay: 10_000,
             refs: [MyApp.Endpoint.HTTP, MyApp.Endpoint.HTTPS]
           ) == spec2
  end

  test "sockets are monitored and shed" do
    Application.ensure_started(:telemetry)

    :telemetry.attach(
      "monitor_start",
      [:socket_drano, :monitor, :start],
      &__MODULE__.handle_event/4,
      %{pid: self()}
    )

    :telemetry.attach(
      "monitor_stop",
      [:socket_drano, :monitor, :stop],
      &__MODULE__.handle_event/4,
      %{pid: self()}
    )

    assert SocketDrano.socket_count() == :undefined

    spec = SocketDrano.child_spec(refs: [], shutdown_delay: 10_000)
    start_supervised!(spec)
    disconnects_pid = spawn_link(fn -> disconnects([]) end)

    refute SocketDrano.draining?()

    sockets =
      Enum.map(1..1000, fn id ->
        spawn_link(fn -> start_socket_process(id, disconnects_pid) end)
        id
      end)

    assert eventually(fn -> SocketDrano.socket_count() == 1000 end, 1000)
    SocketDrano.start_draining()
    assert eventually(fn -> SocketDrano.draining?() == false end, 1000)
    assert eventually(fn -> SocketDrano.socket_count() == 0 end, 1000)
    send(disconnects_pid, {:get_ids, self()})

    receive do
      {:disconnected_ids, ids} ->
        assert Enum.sort(ids) == Enum.sort(sockets)
    after
      10000 ->
        flunk()
    end
  end

  test "socket monitoring and draining with drain_only strategy" do
    Application.ensure_started(:telemetry)

    :telemetry.attach(
      "monitor_start",
      [:socket_drano, :monitor, :start],
      &__MODULE__.handle_event/4,
      %{pid: self()}
    )

    :telemetry.attach(
      "monitor_stop",
      [:socket_drano, :monitor, :stop],
      &__MODULE__.handle_event/4,
      %{pid: self()}
    )

    assert SocketDrano.socket_count() == :undefined

    spec =
      SocketDrano.child_spec(
        refs: [],
        shutdown_delay: 10_000,
        strategy: {:drain_only, 25, 25, 100}
      )

    start_supervised!(spec)
    disconnects_pid = spawn_link(fn -> disconnects([]) end)

    refute SocketDrano.draining?()

    Enum.each(1..100, fn id ->
      spawn_link(fn -> start_socket_process(id, disconnects_pid) end)
    end)

    assert eventually(fn -> SocketDrano.socket_count() == 100 end, 1000)
    SocketDrano.start_draining()
    assert eventually(fn -> SocketDrano.socket_count() == 75 end)
    send(disconnects_pid, {:get_ids, self()})

    receive do
      {:disconnected_ids, ids} ->
        assert length(ids) == 25
    after
      10000 ->
        flunk()
    end
  end

  test "sockets are monitored and deleted from ets on terminate" do
    Application.ensure_started(:telemetry)
    test_pid = self()
    spec = SocketDrano.child_spec(refs: [], shutdown_delay: 10_000)
    start_supervised!(spec)

    socket_pid =
      spawn(fn ->
        receive do
          %Phoenix.Socket.Broadcast{event: "disconnect"} ->
            send(test_pid, :disconnected)
        end
      end)

    assert SocketDrano.socket_count() == 0

    SocketDrano.handle_event(
      [:phoenix, :channel_joined],
      %{},
      %{socket: %{transport: :websocket, transport_pid: socket_pid, endpoint: nil}},
      %{}
    )

    assert SocketDrano.socket_count() == 1

    SocketDrano.handle_event(
      [:phoenix, :channel_joined],
      %{},
      %{socket: %{transport: :websocket, transport_pid: socket_pid, endpoint: nil}},
      %{}
    )

    assert SocketDrano.socket_count() == 1
    SocketDrano.start_draining()
    assert_receive :disconnected, 1000
    refute Process.alive?(socket_pid)
    assert eventually(fn -> SocketDrano.socket_count() == 0 end, 1000)
  end

  test "sockets stop draining if resume_after_drain is set to true" do
    Application.ensure_started(:telemetry)

    :telemetry.attach(
      "monitor_start",
      [:socket_drano, :monitor, :start],
      &__MODULE__.handle_event/4,
      %{pid: self()}
    )

    :telemetry.attach(
      "monitor_stop",
      [:socket_drano, :monitor, :stop],
      &__MODULE__.handle_event/4,
      %{pid: self()}
    )

    assert SocketDrano.socket_count() == :undefined

    spec = SocketDrano.child_spec(refs: [], shutdown_delay: 10_000, resume_after_drain: true)
    start_supervised!(spec)
    disconnects_pid = spawn_link(fn -> disconnects([]) end)

    refute SocketDrano.draining?()

    sockets =
      Enum.map(1..1000, fn id ->
        spawn_link(fn -> start_socket_process(id, disconnects_pid) end)
        id
      end)

    assert eventually(fn -> SocketDrano.socket_count() == 1000 end)

    SocketDrano.start_draining()

    Process.sleep(500)

    send(disconnects_pid, {:get_ids, self()})

    receive do
      {:disconnected_ids, ids} ->
        assert Enum.sort(ids) == Enum.sort(sockets)
    after
      10000 ->
        flunk()
    end

    assert eventually(fn -> false == SocketDrano.draining?() end)
  end

  def start_socket_process(id, monitor_pid) do
    :telemetry.execute([:phoenix, :channel_joined], %{}, %{
      socket: %{transport: :websocket, transport_pid: self(), endpoint: __MODULE__}
    })

    receive do
      %Phoenix.Socket.Broadcast{event: "disconnect"} ->
        send(monitor_pid, {:disconnect, id})
    end
  end

  def disconnects(ids) do
    receive do
      {:disconnect, id} ->
        disconnects([id | ids])

      {:get_ids, caller} ->
        send(caller, {:disconnected_ids, ids})
    end
  end

  def handle_event(event, measurements, meta, config) do
    send(config.pid, {:event, event, measurements, meta, config})
  end

  defp eventually(fun, tries \\ 100, interval \\ 20) do
    case fun.() do
      x when is_nil(x) or false == x ->
        Process.sleep(interval)

        if tries == 0 do
          raise "function returned #{inspect(x)}"
        else
          eventually(fun, tries - 1, interval)
        end

      other ->
        other
    end
  end
end
