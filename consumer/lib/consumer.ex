defmodule Consumer do
  @moduledoc """
  Documentation for `Consumer`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Consumer.hello()
      :world

  """
def start() do

  client_endpoints = ["localhost": 29092]
    group_config = [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: 5,
      rejoin_delay_seconds: 2,
      reconnect_cool_down_seconds: 10
    ]

    config = %{
      client: :kafka_client,
      group_id: "from_zero",
      topics: ["test_topic"],
      cb_module: __MODULE__,
      group_config: group_config,
      consumer_config: [begin_offset: :earliest]
    }

    :brod.start_client(client_endpoints, :kafka_client)
    :brod.start_link_group_subscriber_v2(config)
  end

  def init(_arg, _arg2) do
    {:ok, []}
  end

  def handle_message(message, state) do
    {_kafka_message_set, content, partition, _unkow, _set} = message
    IO.inspect("I recieved a message:")
    IO.inspect(message)
    IO.inspect(content)
    {:ok, :commit, []}
    end
end
