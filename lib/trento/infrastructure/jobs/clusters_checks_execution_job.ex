# SPDX-FileCopyrightText: SUSE LLC
# SPDX-License-Identifier: GPL-3.0-or-later

defmodule Trento.Infrastructure.Jobs.ClustersChecksExecutionJob do
  @moduledoc """
  Requests a checks execution for every registered cluster with selected checks, and schedules
  the next run, see `Trento.Infrastructure.Jobs.ChecksExecutionScheduler`.
  """

  use Oban.Worker, max_attempts: 1, unique: [period: :infinity, states: :incomplete]

  alias Trento.Infrastructure.Jobs.ChecksExecutionScheduler

  @impl Oban.Worker
  def perform(%Oban.Job{} = job) do
    ChecksExecutionScheduler.run(
      __MODULE__,
      job,
      &Trento.Clusters.request_clusters_checks_execution/0
    )
  end
end
