# SPDX-FileCopyrightText: SUSE LLC
# SPDX-License-Identifier: GPL-3.0-or-later

defmodule Trento.Infrastructure.Jobs.ChecksExecutionScheduler do
  @moduledoc """
  Schedules the checks execution jobs, which are not Oban cron entries because their
  frequency is a setting that can change at runtime.

  `schedule_initial/1` inserts the first job of a worker, unless one is already waiting or
  running, and is meant to run at boot. Each job then calls `run/2`, which schedules the next
  job at the scheduled time of the current one plus the checks frequency, whatever the outcome
  of the current job. Failing to schedule the next job is logged and never fails the current one.

  When the checks are disabled the jobs keep being scheduled, at the default frequency, without
  requesting any execution, so that enabling the checks again takes effect.
  """

  import Ecto.Query

  require Logger

  alias Trento.Checks.Frequency
  alias Trento.Repo
  alias Trento.Settings

  @pending_states ~w(available scheduled)

  @spec schedule_initial(module()) :: {:ok, Oban.Job.t()} | :already_scheduled
  def schedule_initial(worker) do
    worker_name = Oban.Worker.to_string(worker)

    pending? =
      Repo.exists?(
        from j in Oban.Job, where: j.worker == ^worker_name and j.state in @pending_states
      )

    if pending? do
      :already_scheduled
    else
      {_enabled, frequency} = current_frequency()

      Oban.insert(worker.new(%{}, scheduled_at: DateTime.add(now(), seconds(frequency))))
    end
  end

  @doc """
  Runs `fun` unless the checks are disabled, and schedules the next job of `worker` afterwards,
  even if `fun` raises, throws or exits.
  """
  @spec run(module(), Oban.Job.t(), (-> any())) :: :ok
  def run(worker, %Oban.Job{} = job, fun) do
    {enabled, _frequency} = current_frequency()

    try do
      if enabled, do: fun.()
      :ok
    after
      schedule_next(worker, job)
    end
  end

  defp schedule_next(worker, %Oban.Job{scheduled_at: scheduled_at}) do
    {_enabled, frequency} = current_frequency()
    now = now()
    next = DateTime.add(scheduled_at, seconds(frequency))

    # After a downtime, do not run once for every missed interval
    next =
      if DateTime.compare(next, now) == :gt, do: next, else: DateTime.add(now, seconds(frequency))

    # The current job is still executing, so the uniqueness of the worker would reject the next
    {:ok, _job} = Oban.insert(worker.new(%{}, scheduled_at: next, unique: false))

    :ok
  rescue
    error ->
      Logger.error(
        "Could not schedule the next #{inspect(worker)} run: " <> Exception.message(error)
      )
  end

  defp current_frequency do
    {:ok, settings} = Settings.get_checks_frequency_settings()

    {settings.enabled, settings.frequency || Frequency.default()}
  end

  defp seconds(frequency), do: Frequency.to_minutes(frequency) * 60

  defp now, do: DateTime.utc_now()
end
