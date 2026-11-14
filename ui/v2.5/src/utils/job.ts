import { useCallback, useEffect, useRef, useState } from "react";
import { getClient, getWSClient, useWSState } from "src/core/StashService";
import {
  Job,
  JobStatus,
  JobStatusUpdateType,
  JobQueueDocument,
  JobQueueQuery,
  useFindJobQuery,
  useJobsSubscribeSubscription,
} from "src/core/generated-graphql";

export type JobFragment = Pick<
  Job,
  "id" | "status" | "subTasks" | "description" | "progress" | "error"
>;

export const useMonitorJob = (
  jobID: string | undefined | null,
  onComplete?: (job?: JobFragment) => void
) => {
  const { state } = useWSState(getWSClient());

  const onCompleteRef = useRef(onComplete);
  useEffect(() => {
    onCompleteRef.current = onComplete;
  }, [onComplete]);

  const jobsSubscribe = useJobsSubscribeSubscription({
    skip: !jobID,
  });
  const {
    data: jobData,
    loading,
    startPolling,
    stopPolling,
  } = useFindJobQuery({
    variables: {
      input: { id: jobID ?? "" },
    },
    fetchPolicy: "network-only",
    skip: !jobID,
  });

  const [job, setJob] = useState<JobFragment | undefined>();

  useEffect(() => {
    if (!jobID) {
      return;
    }

    if (loading) {
      return;
    }

    const j = jobData?.findJob;
    if (j) {
      if (
        j.status === JobStatus.Finished ||
        j.status === JobStatus.Failed ||
        j.status === JobStatus.Cancelled
      ) {
        setJob(undefined);
        onCompleteRef.current?.(j);
      } else {
        setJob(j);
      }
    } else {
      // must've already finished
      setJob(undefined);
      onCompleteRef.current?.();
    }
  }, [jobID, jobData, loading]);

  // monitor job
  useEffect(() => {
    if (!jobID) {
      return;
    }

    if (!jobsSubscribe.data) {
      return;
    }

    const event = jobsSubscribe.data.jobsSubscribe;
    if (event.job.id !== jobID) {
      return;
    }

    if (event.type !== JobStatusUpdateType.Remove) {
      setJob(event.job);
    } else {
      setJob(undefined);
      onCompleteRef.current?.(event.job);
    }
  }, [jobsSubscribe, jobID]);

  // it's possible that the websocket connection isn't present
  // in that case, we'll just poll the server
  useEffect(() => {
    if (!jobID) {
      stopPolling();
      return;
    }

    if (state === "connected") {
      stopPolling();
    } else {
      const defaultPollInterval = 1000;
      startPolling(defaultPollInterval);
    }
  }, [jobID, state, startPolling, stopPolling]);

  return { job };
};

export type QueueJob = Pick<
  Job,
  | "id"
  | "status"
  | "subTasks"
  | "description"
  | "progress"
  | "error"
  | "startTime"
>;

// Tracks the job queue. The list is loaded from the jobQueue query and
// kept up to date from jobsSubscribe events. It is loaded again whenever
// the websocket reconnects, since events sent while it was down are lost.
//
// Events are applied in onData rather than in an effect on the
// subscription's data: when several arrive close together React can
// render once for all of them, and an effect would only see the last one,
// so a dropped Remove would leave a finished job in the list for good.
//
// The query is run directly rather than through useQuery, so that a store
// reset (after each scan or clean) does not refetch it: a snapshot taken
// while jobs were still queued could arrive after their Remove events and
// put them back.
//
// A removed job is kept, showing its final state, for removeDelay ms.
export const useJobQueueState = (removeDelay = 0) => {
  const [queue, setQueue] = useState<QueueJob[]>([]);

  // Events received while a load is in flight. The snapshot may be older
  // than them, so they are applied on top of it.
  const pending = useRef<{
    jobs: Map<string, QueueJob>;
    removed: Set<string>;
  } | null>(null);
  // Bumped by each load, so only the latest one applies its result.
  const loadID = useRef(0);
  const timers = useRef(new Set<ReturnType<typeof setTimeout>>());
  const mounted = useRef(true);

  const load = useCallback(async (attempt = 0) => {
    const id = ++loadID.current;
    const events = {
      jobs: new Map<string, QueueJob>(),
      removed: new Set<string>(),
    };
    pending.current = events;

    let jobs: QueueJob[];
    try {
      const result = await getClient().query<JobQueueQuery>({
        query: JobQueueDocument,
        fetchPolicy: "no-cache",
      });
      jobs = result.data.jobQueue ?? [];
    } catch {
      // The list still has every event applied. Try the snapshot again a
      // few times (a store reset cancels in-flight queries); if the server
      // is down, the reconnect will load it.
      if (id === loadID.current && mounted.current) {
        pending.current = null;
        if (attempt < 3) {
          const timer = setTimeout(() => {
            timers.current.delete(timer);
            load(attempt + 1);
          }, 2000);
          timers.current.add(timer);
        }
      }
      return;
    }

    if (id !== loadID.current || !mounted.current) return;
    pending.current = null;

    const merged = jobs
      .filter((j) => !events.removed.has(j.id))
      .map((j) => events.jobs.get(j.id) ?? j);
    const known = new Set(merged.map((j) => j.id));
    for (const [jobID, j] of events.jobs) {
      if (!known.has(jobID) && !events.removed.has(jobID)) merged.push(j);
    }
    setQueue(merged);
  }, []);

  useEffect(() => {
    mounted.current = true;
    load();

    // Reload after a reconnect. A connection made before the first load
    // finished is already covered by that load.
    let connections = 0;
    const dispose = getWSClient().on("connected", () => {
      connections++;
      if (connections > 1 || !pending.current) load();
    });

    const pendingTimers = timers.current;
    return () => {
      mounted.current = false;
      dispose();
      pendingTimers.forEach(clearTimeout);
      pendingTimers.clear();
    };
  }, [load]);

  useJobsSubscribeSubscription({
    onData: ({ data }) => {
      const event = data.data?.jobsSubscribe;
      if (!event) return;
      const { job } = event;

      const update = (q: QueueJob[]) =>
        q.map((j) => (j.id === job.id ? job : j));
      const remove = (q: QueueJob[]) => q.filter((j) => j.id !== job.id);

      switch (event.type) {
        case JobStatusUpdateType.Add:
          pending.current?.jobs.set(job.id, job);
          setQueue((q) =>
            q.some((j) => j.id === job.id) ? update(q) : q.concat([job])
          );
          break;
        case JobStatusUpdateType.Update:
          pending.current?.jobs.set(job.id, job);
          setQueue(update);
          break;
        case JobStatusUpdateType.Remove: {
          pending.current?.removed.add(job.id);
          if (removeDelay <= 0) {
            setQueue(remove);
            break;
          }
          setQueue(update);
          const timer = setTimeout(() => {
            timers.current.delete(timer);
            setQueue(remove);
          }, removeDelay);
          timers.current.add(timer);
          break;
        }
      }
    },
  });

  return queue;
};
