defmodule SocketDrano do
  @moduledoc """
  Process to gracefully drain Phoenix Socket connections at shutdown.

  `Plug.Cowboy.Drainer` is able to handle draining of connections as they complete during
  a shutdown. Websockets, however, are long-lived connections which may not complete before
  timeout periods are reached, especially if they have a heartbeat to keep alive. This library
  handles both in a single dep to keep things simple.

  In order to gracefully shed Phoenix Sockets, it's necessary to explicitly close them.
  This is useful in scenarios where a container receives a sigterm during a deployment or scaling
  down and you want to avoid a thundering herd on your other containers.

  This module provides a process that during shutdown will initiate shutdown of open Phoenix
  sockets. When the client receives the disconnect message, it will attempt to reconnect, so
  this is most effective when used in combination with your load balancer removing the container
  from the available pool.

  On `start_draining`, SocketDrano will spawn non-blocking processes to disconnect monitored
  socket connections and exit promptly to allow  to do its thing.

  Note: This library does not solve the issue of rebalancing of sockets. That's a tougher
  issue and highly dependent on your load-balancing strategy and infra.

  Socket connections are discovered via telemetry events and monitored for socket closing.

  **Important: This library currently leverages an undocumented internal function in Phoenix
  to achieve its magic of closing local sockets. I may attempt to get this functionality
  explicitly exposed in Phoenix APIs. This could cease working if that internal function
  were to change, but it should not break.**

  ## Usage

  If you run into issues in your test or development environment, you can set the `shutdown_delay`
  to a low value, such as `0` in non-production environments.

  If you are using a shell script to start your application, be sure that it isn't trapping
  OS signals. [Sigterm Propagation](http://veithen.io/2014/11/16/sigterm-propagation.html)

  ## Options

  The following options can be given to the child spec:

    * `:refs` - A list of refs to drain. `:all` is also supported and will drain all cowboy
      listeners, including those started by means other than `Plug.Cowboy`. Required

    * `:shutdown_delay` - How long to wait for connections to drain.
      This number should not exceed the max time before a sigkill is sent by your container
      orchestration settings.
      Defaults to 5000ms.

    * `:drain_check_interval` - How frequently ranch should check for
      all connections to have drained.

    * `:resume_after_drain` - Should we resume listeners after all connections have been
      drained?. This option is useful to stop and drain connections on a node to balance
      its current load and after that accept more connections to keep working.
      Defaults to `false`.


    * `:strategy` - Strategy to drain the sockets. The percentage
      and time should resolve to 100% of connections being drained
      in a time less than the shutdown time.
      Defaults to {:percentage, 25, 100}.

  ## Examples

      # In your application
      def start(_type, _args) do
        children = [
          MyApp.Endpoint,
          {SocketDrano, refs: [MyApp.Endpoint.HTTP]}
        ]

        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        Supervisor.start_link(children, opts)
      end

  ## Strategies

  Only a percentage-based strategy is currently supported. Disconnect batches are made
  according to the percentage size provided. Each batch is processed at the given interval.
  Within a batch, each individual socket disconnect is processed in its own process with
  an added jitter of 1-100ms.

  ## Telemetry

  * `[:socket_drano, :monitor, :start]`
  * `[:socket_drano, :monitor, :stop]`

  """

  # Original connection draining portions
  # Copyright (c) 2013 Plataformatec.

  # Licensed under the Apache License, Version 2.0 (the "License");
  # you may not use this file except in compliance with the License.
  # You may obtain a copy of the License at

  # http://www.apache.org/licenses/LICENSE-2.0

  # Unless required by applicable law or agreed to in writing, software
  # distributed under the License is distributed on an "AS IS" BASIS,
  #   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  # See the License for the specific language governing permissions and
  # limitations under the License.

  use GenServer
  require Logger

  @table __MODULE__

  def child_spec(opts \\ []) do
    opts = ensure_opts(opts)
    :ok = validate_opts!(opts)

    {spec_opts, opts} = Keyword.split(opts, [:id, :shutdown])

    Supervisor.child_spec(
      %{
        id: __MODULE__,
        start: {__MODULE__, :start_link, [opts]},
        type: :worker
      },
      spec_opts
    )
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  def init(opts) do
    opts = ensure_opts(opts)
    :ok = validate_opts!(opts)

    :telemetry.attach(
      :drano_channel_connect,
      [:phoenix, :channel_joined],
      &__MODULE__.handle_event/4,
      %{}
    )

    :persistent_term.put({:socket_drano, :draining}, false)

    :drano_signal_handler.setup(
      shutdown_delay: opts[:shutdown_delay],
      callback: {__MODULE__, :start_draining, []}
    )

    :ets.new(@table, [
      :ordered_set,
      :named_table,
      :public,
      write_concurrency: true,
      read_concurrency: true
    ])

    Process.flag(:trap_exit, true)

    {:ok,
     %{
       strategy: opts[:strategy],
       refs: opts[:refs],
       drain_check_interval: opts[:drain_check_interval],
       resume_after_drain: opts[:resume_after_drain],
       opts: opts
     }}
  end

  def monitor(socket) do
    pid = socket.transport_pid
    endpoint = socket.endpoint

    if :ets.lookup(@table, pid) == [] do
      :telemetry.execute(
        [:socket_drano, :monitor, :start],
        %{start_time: System.system_time()},
        %{endpoint: endpoint, pid: pid}
      )

      :ets.insert(@table, {pid, {endpoint, System.monotonic_time()}})
      GenServer.cast(__MODULE__, {:monitor, pid})
    end

    :ok
  end

  def handle_cast({:monitor, pid}, state) do
    Process.monitor(pid)
    {:noreply, state}
  end

  def handle_cast(:start_draining, %{opts: opts} = state) do
    count = socket_count()
    Logger.info("Starting to drain #{count} sockets")
    start = System.monotonic_time()

    :telemetry.execute(
      [:socket_drano, :draining, :start],
      %{start_time: System.system_time()}
    )

    :persistent_term.put({:socket_drano, :draining}, true)

    drain(state.refs, state.drain_check_interval)
    # sync wait on sockets to drain so we don't let other drains to happen at the same time
    drain_sockets(state.strategy, count, opts)

    if state.resume_after_drain do
      resume_listeners(state.refs)
    end

    :persistent_term.put({:socket_drano, :draining}, false)

    :telemetry.execute(
      [:socket_drano, :draining, :stop],
      %{duration: System.monotonic_time() - start}
    )

    {:noreply, state}
  end

  def handle_cast(_, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    Process.demonitor(ref)

    case :ets.lookup(@table, pid) do
      [] ->
        :ok

      [{pid, {endpoint, start}}] ->
        :telemetry.execute(
          [:socket_drano, :monitor, :stop],
          %{duration: System.monotonic_time() - start},
          %{endpoint: endpoint}
        )

        :ets.delete(@table, pid)
    end

    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    {:stop, reason, state}
  end

  def start_draining() do
    GenServer.cast(__MODULE__, :start_draining)
  end

  def draining? do
    :persistent_term.get({:socket_drano, :draining})
  end

  def socket_count do
    :ets.info(@table, :size)
  end

  def handle_event(
        [:phoenix, :channel_joined],
        _measurements,
        %{socket: %{transport: :websocket}} = meta,
        %{}
      ) do
    monitor(meta.socket)
  end

  def handle_event([:phoenix, :channel_joined], _measurements, _meta, _), do: :ok

  defp drain_sockets(_, 0, _), do: :ok

  defp drain_sockets({:percentage, amount, time}, count, opts) do
    batch_sizes = ceil(count * amount / 100)

    sockets = :ets.tab2list(@table)
    Logger.info("Draining #{count} sockets in batches of #{batch_sizes} every #{time}ms")

    sockets
    |> Enum.chunk_every(batch_sizes)
    |> drain_socket_batch(opts)
    |> Stream.run()
  end

  # drain only `percentage` of the sockets from the socket, and drain them by
  # percentage `batch_size` every `time`ms
  defp drain_sockets({:drain_only, percentage, batch_size_percent, time}, count, opts) do
    socket_drain_size = ceil(count * percentage / 100)
    socket_drain_batch_size = ceil(socket_drain_size * batch_size_percent / 100)

    sockets = :ets.tab2list(@table)

    Logger.info(
      "Draining #{percentage}% of sockets total:#{socket_drain_size} in batches of #{socket_drain_batch_size} every #{time}ms"
    )

    sockets
    |> Enum.take(socket_drain_size)
    |> Enum.chunk_every(socket_drain_batch_size)
    |> drain_socket_batch(opts)
    |> Stream.run()
  end

  defp drain_socket_batch(socket_batches, opts) do
    Task.async_stream(
      socket_batches,
      fn sockets ->
        sockets |> drain_sockets(opts) |> Stream.run()
      end,
      ordered: false,
      timeout: :infinity,
      max_concurrency: Keyword.fetch!(opts, :close_batch_concurrency)
    )
  end

  defp drain_sockets(sockets, opts) do
    Task.async_stream(
      sockets,
      fn {pid, {endpoint, start}} ->
        send(pid, %Phoenix.Socket.Broadcast{event: "disconnect"})

        :telemetry.execute(
          [:socket_drano, :monitor, :stop],
          %{duration: System.monotonic_time() - start},
          %{endpoint: endpoint, pid: pid}
        )
      end,
      ordered: false,
      timeout: :infinity,
      max_concurrency: Keyword.fetch!(opts, :close_socket_concurrency)
    )
  end

  defp drain(:all, drain_check_interval) do
    :ranch.info()
    |> Enum.map(&elem(&1, 0))
    |> drain(drain_check_interval)
  end

  defp drain(refs, drain_check_interval) do
    refs
    |> Enum.filter(&suspend_listener/1)
    |> Enum.each(&wait_for_connections(&1, drain_check_interval))
  end

  defp resume_listeners(:all) do
    :ranch.info()
    |> Enum.map(&elem(&1, 0))
    |> resume_listeners()
  end

  defp resume_listeners(refs) do
    Enum.each(refs, fn ref ->
      Logger.info("Resuming ranch listener #{inspect(ref)}")
      :ranch.resume_listener(ref)
    end)
  end

  defp suspend_listener(ref) do
    Logger.info("Suspending ranch listener #{inspect(ref)}")

    :ranch.suspend_listener(ref) == :ok
  end

  defp wait_for_connections(ref, drain_check_interval) do
    :ranch.wait_for_connections(ref, :==, 0, drain_check_interval)
  end

  defp validate_opts!(opts) do
    validate_strategy!(opts[:strategy])
    validate_refs!(opts[:refs])
    :ok
  end

  defp validate_strategy!({:percentage, percent, time})
       when is_integer(percent) and
              percent > 0 and
              percent <= 100 and
              is_integer(time) and
              time > 0,
       do: :ok

  defp validate_strategy!({:drain_only, percent, batch_size_percent, time})
       when is_integer(percent) and
              is_integer(batch_size_percent) and
              percent > 0 and
              percent <= 100 and
              batch_size_percent > 0 and
              batch_size_percent <= 100 and
              is_integer(time) and
              time > 0,
       do: :ok

  defp validate_strategy!(term) do
    raise ArgumentError,
          "expected strategy to be a valid strategy, such as {:percentage, 10, 250}, got #{inspect(term)}"
  end

  defp validate_refs!(:all), do: :ok
  defp validate_refs!(refs) when is_list(refs), do: :ok

  defp validate_refs!(refs) do
    raise ArgumentError,
          ":refs should be :all or a list of references, got: #{inspect(refs)}"
  end

  defp ensure_opts(opts) do
    default_opts()
    |> Keyword.merge(opts)
  end

  defp default_opts do
    [
      strategy: {:percentage, 25, 100},
      name: __MODULE__,
      drain_check_interval: 1000,
      shutdown_delay: 5000,
      resume_after_drain: false,
      close_batch_concurrency: System.schedulers_online() * 4,
      close_socket_concurrency: System.schedulers_online()
    ]
  end
end
