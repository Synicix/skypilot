"""Tests for KubernetesPodWaitCondition.

A launch whose pods wait on queue admission (Kueue's scheduling gate) or on
the scheduler parks as WAITING instead of holding an API-server executor
worker. The condition decides when the parked request resumes: the resumed
attempt re-enters _wait_for_pods_to_schedule, which owns every outcome
(admitted, scheduled, deleted, timed out). So the condition only has to
notice that *something* changed, never miss it, and never hold up a
cancelled request.
"""
import asyncio
import pickle
import threading
from unittest import mock

import pytest
import urllib3

from sky.provision import constants as prov_constants
from sky.provision.kubernetes import pod_wait_condition
from sky.utils import common_utils
from sky.utils import schemas

_CLUSTER = 'my-cluster-2ea4'


def _pod(name: str,
         *,
         gated: bool = False,
         scheduled: bool = False,
         unschedulable_msg: str = None,
         deleting: bool = False):
    """A pod as the kubernetes client returns it, with every field the
    condition reads set explicitly (auto-created MagicMock attributes are
    truthy and would read as gated/bound/deleting)."""
    pod = mock.MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = {prov_constants.TAG_SKYPILOT_CLUSTER_NAME: _CLUSTER}
    pod.metadata.deletion_timestamp = (mock.MagicMock() if deleting else None)
    if gated:
        gate = mock.MagicMock()
        gate.name = 'kueue.x-k8s.io/admission'
        pod.spec.scheduling_gates = [gate]
    else:
        pod.spec.scheduling_gates = None
    pod.status.phase = 'Pending'
    pod.spec.node_name = 'node-1' if scheduled else None
    conditions = []
    if unschedulable_msg is not None:
        condition = mock.MagicMock()
        condition.type = 'PodScheduled'
        condition.status = 'False'
        condition.reason = 'Unschedulable'
        condition.message = unschedulable_msg
        conditions.append(condition)
    pod.status.conditions = conditions
    return pod


def _serve(monkeypatch, *polls):
    """Serve successive list_namespaced_pod results; the last one repeats.

    Each poll is a list of pods or an exception to raise. Returns the core
    API mock so tests can count polls.
    """
    remaining = list(polls)

    def list_pods(namespace, label_selector=None, **kwargs):
        del namespace, kwargs
        assert label_selector == (
            f'{prov_constants.TAG_SKYPILOT_CLUSTER_NAME}={_CLUSTER}')
        current = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        if isinstance(current, Exception):
            raise current
        result = mock.MagicMock()
        result.items = current
        return result

    core_api = mock.MagicMock()
    core_api.list_namespaced_pod.side_effect = list_pods
    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *a, **kw: core_api)
    return core_api


def _condition(mode: str,
               pods=('pod-0',),
               deadline=None) -> pod_wait_condition.KubernetesPodWaitCondition:
    return pod_wait_condition.KubernetesPodWaitCondition(
        context='kind-q',
        namespace='skypilot-e2e',
        cluster_name_on_cloud=_CLUSTER,
        expected_pod_names=list(pods),
        mode=mode,
        deadline=deadline,
        poll_seconds=10.0)


@pytest.fixture
def no_sleep(monkeypatch):
    """Record sleeps (sync and async) instead of sleeping."""
    sleeps = []

    async def fake_async_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(pod_wait_condition.time, 'sleep', sleeps.append)
    monkeypatch.setattr(pod_wait_condition.asyncio, 'sleep', fake_async_sleep)
    return sleeps


class TestProbe:

    def test_admission_resumes_when_no_pod_gated(self, monkeypatch):
        _serve(monkeypatch, [_pod('pod-0', gated=True)], [_pod('pod-0')])
        condition = _condition(pod_wait_condition.PARK_MODE_ADMISSION)
        assert condition._probe()[0] is False
        assert condition._probe()[0] is True

    def test_admission_waits_while_any_pod_gated(self, monkeypatch):
        _serve(monkeypatch, [_pod('pod-0'), _pod('pod-1', gated=True)])
        condition = _condition(pod_wait_condition.PARK_MODE_ADMISSION,
                               pods=('pod-0', 'pod-1'))
        assert condition._probe()[0] is False

    def test_scheduling_resumes_when_all_scheduled(self, monkeypatch):
        _serve(monkeypatch, [
            _pod('pod-0', scheduled=True),
            _pod('pod-1', unschedulable_msg='0/3 nodes are available')
        ], [_pod('pod-0', scheduled=True),
            _pod('pod-1', scheduled=True)])
        condition = _condition(pod_wait_condition.PARK_MODE_SCHEDULING,
                               pods=('pod-0', 'pod-1'))
        assert condition._probe() == (
            False, 'Waiting for pods to be scheduled: 0/3 nodes are available')
        assert condition._probe()[0] is True

    @pytest.mark.parametrize('mode', [
        pod_wait_condition.PARK_MODE_ADMISSION,
        pod_wait_condition.PARK_MODE_SCHEDULING
    ])
    def test_resumes_once_a_pod_has_been_missing_for_the_grace(
            self, monkeypatch, mode):
        """Same grace as the in-worker wait: one list that omits a pod may
        be a stale read, so it is a missed poll, not a deletion."""
        _serve(monkeypatch, [_pod('pod-1', gated=True)])
        now = [1000.0]
        monkeypatch.setattr(pod_wait_condition.time, 'time', lambda: now[0])
        condition = _condition(mode, pods=('pod-0', 'pod-1'))
        assert condition._probe() == (False, None)
        now[0] += pod_wait_condition.MISSING_POD_GRACE_SECONDS - 1
        assert condition._probe() == (False, None)
        now[0] += 1
        assert condition._probe() == (True, None)

    def test_a_pod_listed_again_starts_the_grace_over(self, monkeypatch):
        grace = pod_wait_condition.MISSING_POD_GRACE_SECONDS
        _serve(monkeypatch, [_pod('pod-1', gated=True)],
               [_pod('pod-0', gated=True),
                _pod('pod-1', gated=True)], [_pod('pod-1', gated=True)])
        now = [1000.0]
        monkeypatch.setattr(pod_wait_condition.time, 'time', lambda: now[0])
        condition = _condition(pod_wait_condition.PARK_MODE_ADMISSION,
                               pods=('pod-0', 'pod-1'))
        assert condition._probe()[0] is False  # missing
        now[0] += grace - 1
        assert condition._probe()[0] is False  # back: a stale read
        now[0] += 2
        assert condition._probe()[0] is False  # missing again, new streak
        now[0] += grace
        assert condition._probe()[0] is True

    @pytest.mark.parametrize('mode', [
        pod_wait_condition.PARK_MODE_ADMISSION,
        pod_wait_condition.PARK_MODE_SCHEDULING
    ])
    def test_resumes_when_pod_has_deletion_timestamp(self, monkeypatch, mode):
        _serve(monkeypatch, [_pod('pod-0', gated=True, deleting=True)])
        condition = _condition(mode)
        assert condition._probe()[0] is True

    def test_resumes_at_deadline(self, monkeypatch):
        core_api = _serve(monkeypatch, [_pod('pod-0', gated=True)])
        condition = _condition(pod_wait_condition.PARK_MODE_ADMISSION,
                               deadline=100.0)
        monkeypatch.setattr(pod_wait_condition.time, 'time', lambda: 99.0)
        assert condition._probe()[0] is False
        monkeypatch.setattr(pod_wait_condition.time, 'time', lambda: 101.0)
        assert condition._probe()[0] is True
        # Past the deadline the answer is known without asking the cluster.
        assert core_api.list_namespaced_pod.call_count == 1

    @pytest.mark.parametrize('error', [
        urllib3.exceptions.MaxRetryError(None, '/api/v1/pods'),
        pod_wait_condition.kubernetes.api_exception()(status=0, reason='SSL'),
    ])
    def test_transport_error_is_a_missed_poll(self, monkeypatch, error):
        _serve(monkeypatch, error)
        condition = _condition(pod_wait_condition.PARK_MODE_ADMISSION)
        assert condition._probe() == (False, None)

    @pytest.mark.parametrize('status', [429, 500, 502, 503, 504])
    def test_overloaded_api_server_is_a_missed_poll(self, monkeypatch, status):
        """Hundreds of parked launches share one API server; a 429 or 5xx
        must not resume them all at once into workers that fail the same
        way. It counts against the transport-error grace like a timeout."""
        _serve(
            monkeypatch,
            pod_wait_condition.kubernetes.api_exception()(status=status,
                                                          reason='busy'))
        condition = _condition(pod_wait_condition.PARK_MODE_ADMISSION)
        assert condition._probe() == (False, None)
        assert condition._transport_error_since is not None

    @pytest.mark.parametrize('status', [401, 403, 404])
    def test_api_error_response_resumes_now(self, monkeypatch, status):
        """An answer (revoked credential, namespace gone) is what the wait
        itself raises at once; resume so the resumed attempt reports it
        instead of parking forever."""
        _serve(
            monkeypatch,
            pod_wait_condition.kubernetes.api_exception()(status=status,
                                                          reason='denied'))
        condition = _condition(pod_wait_condition.PARK_MODE_ADMISSION)
        assert condition._probe() == (True, None)

    def test_persistent_transport_errors_resume_after_grace(self, monkeypatch):
        """Same budget as the in-worker wait: a streak of transport errors
        longer than the grace ends the park so the error surfaces."""
        _serve(monkeypatch, urllib3.exceptions.MaxRetryError(None, '/'))
        now = [1000.0]
        monkeypatch.setattr(pod_wait_condition.time, 'time', lambda: now[0])
        condition = _condition(pod_wait_condition.PARK_MODE_SCHEDULING)
        grace = pod_wait_condition.POD_POLL_TRANSPORT_ERROR_GRACE_SECONDS
        assert condition._probe()[0] is False
        now[0] += grace - 1
        assert condition._probe()[0] is False
        now[0] += 2
        assert condition._probe()[0] is True

    def test_transport_error_streak_resets_after_a_good_poll(self, monkeypatch):
        grace = pod_wait_condition.POD_POLL_TRANSPORT_ERROR_GRACE_SECONDS
        error = urllib3.exceptions.MaxRetryError(None, '/')
        _serve(monkeypatch, error, [_pod('pod-0', gated=True)], error, error)
        now = [1000.0]
        monkeypatch.setattr(pod_wait_condition.time, 'time', lambda: now[0])
        condition = _condition(pod_wait_condition.PARK_MODE_ADMISSION)
        assert condition._probe()[0] is False  # error, streak starts
        now[0] += grace - 1
        assert condition._probe()[0] is False  # good poll, streak ends
        now[0] += 2
        assert condition._probe()[0] is False  # error, new streak
        now[0] += grace - 1
        assert condition._probe()[0] is False  # still inside the new grace


class TestWait:

    @pytest.mark.asyncio
    async def test_wait_async_returns_false_when_cancelled(
            self, monkeypatch, no_sleep):
        core_api = _serve(monkeypatch, [_pod('pod-0', gated=True)])

        async def is_cancelled():
            return True

        result = await _condition(pod_wait_condition.PARK_MODE_ADMISSION
                                 ).wait_async(is_cancelled=is_cancelled,
                                              fallback_wait_seconds=30)
        assert result is False
        assert core_api.list_namespaced_pod.call_count == 0
        assert no_sleep == []

    @pytest.mark.asyncio
    async def test_wait_async_first_probe_is_immediate(self, monkeypatch,
                                                       no_sleep):
        """The gate may come off between the park and the first poll; the
        condition must resume at once rather than wait out an interval."""
        _serve(monkeypatch, [_pod('pod-0')])

        async def is_cancelled():
            return False

        result = await _condition(pod_wait_condition.PARK_MODE_ADMISSION
                                 ).wait_async(is_cancelled=is_cancelled,
                                              fallback_wait_seconds=30)
        assert result is True
        assert no_sleep == []

    @pytest.mark.asyncio
    async def test_wait_async_polls_until_admitted(self, monkeypatch, no_sleep):
        _serve(monkeypatch, [_pod('pod-0', gated=True)],
               [_pod('pod-0', gated=True)], [_pod('pod-0')])

        async def is_cancelled():
            return False

        result = await _condition(pod_wait_condition.PARK_MODE_ADMISSION
                                 ).wait_async(is_cancelled=is_cancelled,
                                              fallback_wait_seconds=30)
        assert result is True
        assert no_sleep == [10.0, 10.0]

    @pytest.mark.asyncio
    async def test_scheduling_reason_refreshed_on_change(
            self, monkeypatch, no_sleep):
        del no_sleep
        _serve(monkeypatch, [_pod('pod-0', unschedulable_msg='a')],
               [_pod('pod-0', unschedulable_msg='a')],
               [_pod('pod-0', unschedulable_msg='b')],
               [_pod('pod-0', scheduled=True)])
        reasons = []

        async def is_cancelled():
            return False

        async def update_status_msg(reason):
            reasons.append(reason)

        result = await _condition(pod_wait_condition.PARK_MODE_SCHEDULING
                                 ).wait_async(
                                     is_cancelled=is_cancelled,
                                     fallback_wait_seconds=30,
                                     update_status_msg=update_status_msg)
        assert result is True
        # Same wording as the message the launch parked with, so the status
        # reads the same before and after a refresh.
        assert reasons == [
            'Waiting for pods to be scheduled: a',
            'Waiting for pods to be scheduled: b'
        ]

    @pytest.mark.asyncio
    async def test_wait_async_without_update_status_msg(self, monkeypatch,
                                                        no_sleep):
        """The scheduler omits update_status_msg for conditions whose wait
        predates it; the condition must not require it."""
        del no_sleep
        _serve(monkeypatch, [_pod('pod-0', unschedulable_msg='a')],
               [_pod('pod-0', scheduled=True)])

        async def is_cancelled():
            return False

        assert await _condition(pod_wait_condition.PARK_MODE_SCHEDULING
                               ).wait_async(is_cancelled=is_cancelled,
                                            fallback_wait_seconds=30) is True

    def test_wait_and_wait_async_agree(self, monkeypatch, no_sleep):
        timeline = ([_pod('pod-0', unschedulable_msg='a')
                    ], [_pod('pod-0', unschedulable_msg='b')],
                    [_pod('pod-0', scheduled=True)])

        sync_api = _serve(monkeypatch, *timeline)
        sync_reasons = []
        sync_result = _condition(pod_wait_condition.PARK_MODE_SCHEDULING).wait(
            is_cancelled=lambda: False,
            fallback_wait_seconds=30,
            update_status_msg=sync_reasons.append)
        sync_sleeps = list(no_sleep)
        no_sleep.clear()

        async_api = _serve(monkeypatch, *timeline)
        async_reasons = []

        async def is_cancelled():
            return False

        async def update_status_msg(reason):
            async_reasons.append(reason)

        async_result = asyncio.run(
            _condition(pod_wait_condition.PARK_MODE_SCHEDULING).wait_async(
                is_cancelled=is_cancelled,
                fallback_wait_seconds=30,
                update_status_msg=update_status_msg))

        assert sync_result is async_result is True
        assert sync_reasons == async_reasons == [
            'Waiting for pods to be scheduled: a',
            'Waiting for pods to be scheduled: b'
        ]
        assert sync_sleeps == no_sleep == [10.0, 10.0]
        assert (sync_api.list_namespaced_pod.call_count ==
                async_api.list_namespaced_pod.call_count == 3)

    def test_wait_returns_false_when_cancelled_mid_wait(self, monkeypatch,
                                                        no_sleep):
        del no_sleep
        _serve(monkeypatch, [_pod('pod-0', gated=True)])
        answers = iter([False, False, True])
        assert _condition(pod_wait_condition.PARK_MODE_ADMISSION).wait(
            is_cancelled=lambda: next(answers),
            fallback_wait_seconds=30) is False


def test_default_poll_interval_is_long():
    """Every parked launch on the server polls at this rate, for hours; the
    launch already waited out a grace before parking, so a resume delayed by
    one interval costs nothing next to the queue."""
    assert pod_wait_condition.DEFAULT_POLL_SECONDS == 30.0
    condition = pod_wait_condition.KubernetesPodWaitCondition(
        context=None,
        namespace='ns',
        cluster_name_on_cloud=_CLUSTER,
        expected_pod_names=['pod-0'],
        mode=pod_wait_condition.PARK_MODE_ADMISSION,
        deadline=None)
    assert condition.poll_seconds == 30.0


@pytest.mark.asyncio
async def test_probes_run_on_their_own_bounded_pool(monkeypatch, no_sleep):
    """The list call runs off the event loop, on the probe pool rather than
    the loop's default executor: a slow API server then delays resumes,
    not the cancellation checks every parked request shares."""
    del no_sleep
    threads = []
    real_probe = pod_wait_condition.KubernetesPodWaitCondition._probe

    def probe(self):
        threads.append(threading.current_thread().name)
        return real_probe(self)

    monkeypatch.setattr(pod_wait_condition.KubernetesPodWaitCondition, '_probe',
                        probe)
    _serve(monkeypatch, [_pod('pod-0', gated=True)], [_pod('pod-0')])

    async def is_cancelled():
        return False

    assert await _condition(pod_wait_condition.PARK_MODE_ADMISSION).wait_async(
        is_cancelled=is_cancelled, fallback_wait_seconds=30) is True
    assert len(threads) == 2
    assert all(name.startswith('sky-parked-launch-probe') for name in threads)
    assert (pod_wait_condition._get_probe_pool()._max_workers ==
            pod_wait_condition._PROBE_THREADS)


def test_condition_pickles():
    """The condition rides on ExecutionPausedError across the executor's
    process boundary."""
    condition = _condition(pod_wait_condition.PARK_MODE_SCHEDULING,
                           pods=('pod-0', 'pod-1'),
                           deadline=123.5)
    restored = pickle.loads(pickle.dumps(condition))
    assert type(restored) is pod_wait_condition.KubernetesPodWaitCondition
    assert restored.__dict__ == condition.__dict__


def test_condition_is_async_capable():
    """The executor uses wait_async (a coroutine on its shared loop, no OS
    thread per parked request) only when it is a coroutine function."""
    assert asyncio.iscoroutinefunction(
        pod_wait_condition.KubernetesPodWaitCondition.wait_async)


@pytest.mark.parametrize('config', [
    {
        'kubernetes': {
            'park_queued_launches': False
        }
    },
    {
        'kubernetes': {
            'context_configs': {
                'kind-q': {
                    'park_queued_launches': True
                }
            }
        }
    },
])
def test_park_queued_launches_is_valid_config(config):
    common_utils.validate_schema(config, schemas.get_config_schema(),
                                 'Invalid config YAML: ')


def test_park_queued_launches_must_be_boolean():
    with pytest.raises(ValueError, match='park_queued_launches'):
        common_utils.validate_schema(
            {'kubernetes': {
                'park_queued_launches': 'yes'
            }}, schemas.get_config_schema(), 'Invalid config YAML: ')
