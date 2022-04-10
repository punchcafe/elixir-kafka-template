defmodule Producer do
  @moduledoc """
  Documentation for `Producer`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Producer.hello()
      :world

  """
def publish(topic, partition_key, message) do
    {:ok, count} = :brod.get_partitions_count(:kafka_client, topic)

    :brod.produce_sync(
      :kafka_client,
      topic,
      :erlang.phash2(partition_key, count),
      partition_key,
      message
    )
  end

  def start(_, _) do
    create("test_topic")
    {:ok, spawn(&looper/0)}
  end

  defp looper(), do: looper()

defp create(name) do
    topic_config = [
      %{
        config_entries: [],
        num_partitions: 6,
        replica_assignment: [],
        replication_factor: 1,
        topic: name
      }
    ]

    :brod.create_topics(
      ["localhost": 29092],
      topic_config,
      %{timeout: 1_000}
    )
  end
end
