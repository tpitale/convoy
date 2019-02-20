defmodule Convoy.SubscriptionProducer do
  use GenStage

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :"subscription_producer_#{opts[:stream]}")
  end

  def init(opts) do
    {:producer, {0, stream: opts[:stream]}}
  end

  def handle_demand(incoming_demand, {current_demand, stream}) do
    dispatch_events({incoming_demand + current_demand, stream})
  end

  defp dispatch_events({0, stream}) do
    {:noreply, [], {0, stream}}
  end

  defp dispatch_events({demand, stream}) do
    case Convoy.Queue.get(stream) do
      records ->
        remaining_demand = max(demand - (records |> length()), 0)
        {:noreply, records, {remaining_demand, stream}}
    end
  end
end
