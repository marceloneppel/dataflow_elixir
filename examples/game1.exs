use Dataflow
alias Dataflow.Transforms.{Core, IO}

defmodule GameActionInfo do
  defstruct [:user, :team, :score, :timestamp]

  @doc """
  Parses the raw game event info into GameActionInfo structs. Each event line has the following format:
  `username,teamname,score,timestamp_in_ms,readable_time`, e.g.
  `user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224`
  The human-readable time string is not used here.
  """
  def parse(text) do
    #TODO count parse errors in an aggregator/!metric!
    require Logger

    components = String.split text, ","

    case components do
      [user, team, score_text, timestamp_text, _hrts] ->
        with {score, ""} <- Integer.parse(score_text),
          {timestamp, ""} <- Integer.parse(timestamp_text)
        do
          {:ok, %__MODULE__{user: user, team: team, score: score, timestamp: Time.timestamp(timestamp, :milliseconds)}}
        else
          _ ->
            Logger.error "Integer parsing of GameActionInfo event failed"
            :error
        end
      _ ->
        Logger.error "Invalid format of GameActionInfo event"
        :error
    end

  end
end

deftransform parse_events do
  _, input ->
    input
    ~> Core.map(fn text ->
        case GameActionInfo.parse(text) do
          {:ok, el} -> [el]
          :error -> []
        end
      end)
end

@moduledoc """
A transform to extract key/score information from GameActionInfo, and sum the scores.
The constructor arg determines whether 'team' or 'user' info is extracted.
"""
deftransform extract_and_sum_score, params: [:key] do
  %{key: key}, input ->
    input
    ~> Core.map(fn el -> {el[key], el.score} end)
    ~> Aggregation.sum_per_key
end

p = Pipeline.new runner: Dataflow.DirectRunner

p
~> "read_events" -- IO.read_file("~/temp_dataflow_data/gaming_data/gaming_data1.csv")
~> "parse_game_event" -- parse_events
~> "extract_user_score" -- extract_and_sum_score(:user)
~> "format" -- Core.map(fn {k, v} -> "#{k}: #{v}" end)
~> "write_user_score_sums" -- IO.write_file("examples/data/game1out.txt")

Pipeline.run p, sync: true
