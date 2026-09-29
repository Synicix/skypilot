"""Resuming a Kubernetes launch that parked while its pods were waiting.

A launch whose pods are held by a queue's scheduling gate, or that the
scheduler cannot place under an indefinite provision_timeout, parks instead
of holding an API-server executor worker for the whole wait (see
``_wait_for_pods_to_schedule`` in ``instance.py``). ``KubernetesPodWaitCondition``
is the continue-condition it parks with: the executor drives it from the
server's main process, so it and the helpers it needs live here rather than
in the provisioning module. The class path is also what a parked request's
pickled exception carries, so it should stay put.
"""
import asyncio
import concurrent.futures
import threading
import time
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from sky import sky_logging
from sky.adaptors import kubernetes
from sky.provision import constants
from sky.utils import common_utils
from sky.utils import timeline

logger = sky_logging.init_logger(__name__)

# Request timeout for the pod polling loops (_wait_for_pods_to_schedule /
# _wait_for_pods_to_run) and the parked-launch probe: (connect, read)
# seconds. Without a request timeout, a connection that stops receiving data
# without being closed (e.g. silently dropped by a NAT/LB after an idle or
# lifetime limit) blocks the poll forever: the loop stops iterating and the
# launch hangs until provision_timeout, which can be hours in
# autoscaling/queueing setups. The read timeout bounds each socket read
# (idle time), not the whole response, so large pod lists that stream slowly
# are unaffected.
POD_POLL_REQUEST_TIMEOUT = (5, 30)
# How long a continuous streak of pod-poll transport errors may last before
# it surfaces as an error. A transient failure (timeout, dropped connection)
# is treated as a missed poll and retried, but a persistently unreachable API
# server should still surface instead of retrying silently forever. The
# budget is wall-clock rather than attempt-based so that fast-failing errors
# (e.g. connection refused) get the same tolerance as slow read timeouts.
POD_POLL_TRANSPORT_ERROR_GRACE_SECONDS = 180
# How long an expected pod may be continuously missing -- absent from the pod
# list, or listed with a deletion timestamp -- before provisioning is failed.
# This is not a recovery window: a pod SkyPilot created and then saw
# disappear never comes back, because nothing recreates it, and a pod that
# shows up later under the same name belongs to a different launch. It buys
# two things that cost nothing to wait for. First, the deleter usually writes
# down why *after* the deletion: a queue controller records its eviction
# through an asynchronous event recorder, so failing on the first poll that
# misses the pod can beat the explanation to the API server and report no
# cause for something that had one. Second, a single list response that omits
# the pod -- a caching proxy or a virtual-cluster syncer serving a partial or
# stale view -- must not by itself fail a launch. Ten seconds covers both, and
# the census behind this change found nothing that a longer wait would have
# saved.
MISSING_POD_GRACE_SECONDS = 10
# API error responses that say "not now" rather than "no": the poll is
# retried like a transport error. Anything else (401, 403, 404, ...) is an
# answer the launch has to act on.
_RETRIABLE_API_STATUSES = frozenset({429, 500, 502, 503, 504})

# What a parked launch is waiting for (see KubernetesPodWaitCondition).
PARK_MODE_ADMISSION = 'admission'
PARK_MODE_SCHEDULING = 'scheduling'

# How often a parked launch looks at its pods. Long on purpose: the waits
# this covers last minutes to hours, the launch already paid a grace period
# before parking, and every parked launch on the server polls at this rate.
DEFAULT_POLL_SECONDS = 30.0
# Probes run on their own small thread pool rather than the event loop's
# default executor, which the executor shares with every parked request's
# cancellation checks and requeues. A slow API server then delays resumes,
# not cancellations, and the number of concurrent list calls it sees from
# parked launches is bounded however many there are.
_PROBE_THREADS = 8
_probe_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
_probe_pool_lock = threading.Lock()


def _get_probe_pool() -> concurrent.futures.ThreadPoolExecutor:
    global _probe_pool
    with _probe_pool_lock:
        if _probe_pool is None:
            _probe_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=_PROBE_THREADS,
                thread_name_prefix='sky-parked-launch-probe')
        return _probe_pool


def pod_is_scheduled(pod) -> bool:
    """Whether the kube-scheduler has bound this pod to a node.

    The scheduler sets ``spec.nodeName`` (and the ``PodScheduled`` status
    condition to ``True``) the moment it places a pod -- i.e. capacity has
    been found. The kubelet on the target node only later populates
    ``status.container_statuses`` / ``host_ip`` once it picks the pod up and
    starts the sandbox. That kubelet pickup can occasionally lag past
    ``provision_timeout`` when the control plane is slow to propagate the
    binding to the kubelet, even though the pod is already bound to a node.

    We treat a bound pod as scheduled so that provisioning hands off to
    ``_wait_for_pods_to_run`` (which waits for containers without the short
    ``provision_timeout``) instead of failing over as if the cluster were out
    of resources. A genuinely unschedulable pod keeps ``PodScheduled`` False
    and no ``nodeName``, so it stays in the scheduling wait loop.
    """
    # Running/Succeeded/Failed pods are clearly past scheduling; Failed pods
    # are surfaced as errors later in _wait_for_pods_to_run.
    if pod.status.phase != 'Pending':
        return True
    # spec.nodeName is set atomically when the scheduler binds the pod.
    if pod.spec.node_name:
        return True
    # Fall back to the PodScheduled status condition.
    for condition in (pod.status.conditions or []):
        if condition.type == 'PodScheduled' and condition.status == 'True':
            return True
    return False


def unschedulable_message(pod) -> Optional[str]:
    """The scheduler's explanation for a pod it cannot place, if any."""
    for condition in (pod.status.conditions or []):
        if (condition.type == 'PodScheduled' and condition.status == 'False' and
                condition.reason == 'Unschedulable'):
            return condition.message or condition.reason
    return None


def scheduling_wait_message(unschedulable_msg: Optional[str]) -> str:
    """The status a launch waiting for the scheduler shows.

    Shared by the park and the condition's refreshes, so the status reads
    the same before and after the scheduler's reason changes.
    """
    if unschedulable_msg:
        return f'Waiting for pods to be scheduled: {unschedulable_msg}'
    return 'Waiting for pods to be scheduled'


@timeline.event
def is_transport_error(e: Exception) -> bool:
    """Whether ``e`` is a failure of the HTTP transport to the API server.

    urllib3 transport errors propagate raw out of the kubernetes client,
    with one exception: the client wraps ``urllib3.exceptions.SSLError``
    into an ``ApiException`` with ``status=0`` (no HTTP response was
    received). An ``ApiException`` with a real HTTP status is an API error
    response, not a transport failure.
    """
    if isinstance(e, kubernetes.api_exception()):
        return e.status == 0
    return isinstance(e, kubernetes.urllib3_http_error())


def is_retriable_poll_error(e: Exception) -> bool:
    """Whether a failed pod poll should be retried rather than acted on.

    Transport failures, and the API server answering that it is overloaded
    or briefly broken (429, 5xx). The latter matters for a parked launch:
    hundreds of them share one API server, and if each treated a 429 as a
    reason to resume they would all resume at once, into workers, to fail
    with the same 429.
    """
    if is_transport_error(e):
        return True
    return (isinstance(e, kubernetes.api_exception()) and
            e.status in _RETRIABLE_API_STATUSES)


class KubernetesPodWaitCondition:
    """Resume a parked launch once its pods stop waiting.

    Attached to the ``exceptions.ExecutionPausedError`` that
    _wait_for_pods_to_schedule raises when a launch would otherwise sit in an
    executor worker for as long as a queue takes to admit its pods (mode
    ``admission``: pods held by a scheduling gate, e.g. Kueue's) or the
    scheduler takes to find room for them (mode ``scheduling``: pods
    Unschedulable under an indefinite provision_timeout). Implements the
    duck-typed continue-condition contract (see
    ``sky/server/requests/continue_condition.py``), including ``wait_async``,
    so any number of parked launches cost coroutines rather than threads.

    Deliberately shallow: it resumes on *any* change that the resumed attempt
    has to act on -- admitted or scheduled, a pod gone or being deleted, the
    admission deadline passed -- and leaves every outcome to the resumed
    _wait_for_pods_to_schedule, which already reports each of them. Errors
    follow that wait's rules too, so that a park can always end: an error
    response resumes at once (the resumed attempt raises it), while transport
    errors and an overloaded API server are missed polls until a streak
    outlasts POD_POLL_TRANSPORT_ERROR_GRACE_SECONDS. A pod missing from one
    list is a missed poll as well, for MISSING_POD_GRACE_SECONDS, as it is in
    that wait: one stale read must not end a wait of hours.

    Instances cross the executor's process boundary on the exception, so
    every attribute is plain data.
    """

    def __init__(self,
                 *,
                 context: Optional[str],
                 namespace: str,
                 cluster_name_on_cloud: str,
                 expected_pod_names: List[str],
                 mode: str,
                 deadline: Optional[float],
                 poll_seconds: float = DEFAULT_POLL_SECONDS) -> None:
        self.context = context
        self.namespace = namespace
        self.cluster_name_on_cloud = cluster_name_on_cloud
        self.expected_pod_names = list(expected_pod_names)
        self.mode = mode
        # Absolute epoch seconds; None waits indefinitely.
        self.deadline = deadline
        self.poll_seconds = poll_seconds
        # Start of the current streak of transport errors, if any.
        self._transport_error_since: Optional[float] = None
        # When each expected pod was first missing from the list, for those
        # currently missing.
        self._missing_since: Dict[str, float] = {}

    def _probe(self) -> Tuple[bool, Optional[str]]:
        """One look at the pods: (resume now?, current waiting reason)."""
        if self.deadline is not None and time.time() >= self.deadline:
            return True, None
        try:
            pods = kubernetes.core_api(self.context).list_namespaced_pod(
                self.namespace,
                label_selector=(f'{constants.TAG_SKYPILOT_CLUSTER_NAME}='
                                f'{self.cluster_name_on_cloud}'),
                _request_timeout=POD_POLL_REQUEST_TIMEOUT).items
        except (kubernetes.api_exception(),
                kubernetes.urllib3_http_error()) as e:
            if not is_retriable_poll_error(e):
                return True, None
            now = time.time()
            if self._transport_error_since is None:
                self._transport_error_since = now
            logger.debug('Parked launch of cluster '
                         f'{self.cluster_name_on_cloud!r}: pod poll failed, '
                         f'will retry: {common_utils.format_exception(e)}')
            return (now - self._transport_error_since >=
                    POD_POLL_TRANSPORT_ERROR_GRACE_SECONDS), None
        self._transport_error_since = None
        now = time.time()
        pods_by_name = {pod.metadata.name: pod for pod in pods}
        present = []
        for name in self.expected_pod_names:
            pod = pods_by_name.get(name)
            if pod is None:
                self._missing_since.setdefault(name, now)
            else:
                self._missing_since.pop(name, None)
                present.append(pod)
        if any(pod.metadata.deletion_timestamp is not None for pod in present):
            return True, None
        if self._missing_since:
            return any(now - since >= MISSING_POD_GRACE_SECONDS
                       for since in self._missing_since.values()), None
        if self.mode == PARK_MODE_ADMISSION:
            return not any(pod.spec.scheduling_gates for pod in present), None
        unscheduled = [pod for pod in present if not pod_is_scheduled(pod)]
        if not unscheduled:
            return True, None
        return False, scheduling_wait_message(
            unschedulable_message(unscheduled[0]))

    def wait(self,
             *,
             is_cancelled: Callable[[], bool],
             fallback_wait_seconds: float,
             update_status_msg: Optional[Callable[[str], None]] = None) -> bool:
        del fallback_wait_seconds  # The pods are the signal.
        last_reason: Optional[str] = None
        while True:
            if is_cancelled():
                return False
            resume, reason = self._probe()
            if resume:
                return True
            if (update_status_msg is not None and reason is not None and
                    reason != last_reason):
                update_status_msg(reason)
                last_reason = reason
            time.sleep(self.poll_seconds)

    async def wait_async(
        self,
        *,
        is_cancelled: Callable[[], Awaitable[bool]],
        fallback_wait_seconds: float,
        update_status_msg: Optional[Callable[[str], Awaitable[None]]] = None
    ) -> bool:
        del fallback_wait_seconds  # The pods are the signal.
        loop = asyncio.get_running_loop()
        last_reason: Optional[str] = None
        while True:
            if await is_cancelled():
                return False
            resume, reason = await loop.run_in_executor(_get_probe_pool(),
                                                        self._probe)
            if resume:
                return True
            if (update_status_msg is not None and reason is not None and
                    reason != last_reason):
                await update_status_msg(reason)
                last_reason = reason
            await asyncio.sleep(self.poll_seconds)
