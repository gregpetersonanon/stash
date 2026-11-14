import { useEffect, useRef, useState } from "react";
import { getWSClient, useWSState } from "src/core/StashService";
import {
  Job,
  JobStatus,
  JobStatusUpdateType,
  useFindJobQuery,
  useJobQueueQuery,
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

// Tracks the job queue. The list is loaded once from the jobQueue query and
// then kept up to date from jobsSubscribe events.
//
// Events are applied in onData rather than in an effect on the
// subscription's data: when several arrive close together React can
// render once for all of them, and an effect would only see the last one,
// so a dropped Remove would leave a finished job in the list for good.
//
// Later results of the query (a store reset refetches it) are ignored: a
// snapshot taken while jobs were still queued could arrive after their
// Remove events and put them back.
//
// A removed job is kept, showing its final state, for removeDelay ms.
export const useJobQueueState = (removeDelay = 0) => {
  const [queue, setQueue] = useState<QueueJob[]>([]);
  const loaded = useRef(false);
  // Jobs removed before the initial query result arrived, so the result
  // does not bring them back.
  const removed = useRef(new Set<string>());
  const timers = useRef(new Set<ReturnType<typeof setTimeout>>());

  const jobQueue = useJobQueueQuery({ fetchPolicy: "no-cache" });

  useEffect(() => {
    const initial = jobQueue.data?.jobQueue;
    if (loaded.current || !initial) return;
    loaded.current = true;

    // Merge with anything the subscription has already delivered. The
    // updater runs later, so it gets its own copy of the removed set.
    const gone = new Set(removed.current);
    removed.current.clear();
    setQueue((current) => {
      const byID = new Map(current.map((j) => [j.id, j]));
      const merged = initial
        .filter((j) => !gone.has(j.id))
        .map((j) => byID.get(j.id) ?? j);
      const known = new Set(merged.map((j) => j.id));
      return merged.concat(current.filter((j) => !known.has(j.id)));
    });
  }, [jobQueue.data]);

  useEffect(() => {
    const pending = timers.current;
    return () => {
      pending.forEach(clearTimeout);
      pending.clear();
    };
  }, []);

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
          setQueue((q) =>
            q.some((j) => j.id === job.id) ? update(q) : q.concat([job])
          );
          break;
        case JobStatusUpdateType.Update:
          setQueue(update);
          break;
        case JobStatusUpdateType.Remove: {
          if (!loaded.current) removed.current.add(job.id);
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
