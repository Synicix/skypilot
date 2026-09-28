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
from unittest import mock

import pytest
import urllib3

from sky.provision import constants as prov_constants
from sky.provision.kubernetes import instance
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
               deadline=None) -> instance.KubernetesPodWaitCondition:
    return instance.KubernetesPodWaitCondition(context='kind-q',
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

    monkeypatch.setattr(instance.time, 'sleep', sleeps.append)
    monkeypatch.setattr(instance.asyncio, 'sleep', fake_async_sleep)
    return sleeps


class TestProbe:

    def test_admission_resumes_when_no_pod_gated(self, monkeypatch):
        _serve(monkeypatch, [_pod('pod-0', gated=True)], [_pod('pod-0')])
        condition = _condition(instance.PARK_MODE_ADMISSION)
        assert condition._probe()[0] is False
        assert condition._probe()[0] is True

    def test_admission_waits_while_any_pod_gated(self, monkeypatch):
        _serve(monkeypatch, [_pod('pod-0'), _pod('pod-1', gated=True)])
        condition = _condition(instance.PARK_MODE_ADMISSION,
                               pods=('pod-0', 'pod-1'))
        assert condition._probe()[0] is False

    def test_scheduling_resumes_when_all_scheduled(self, monkeypatch):
        _serve(monkeypatch, [
            _pod('pod-0', scheduled=True),
            _pod('pod-1', unschedulable_msg='0/3 nodes are available')
        ], [_pod('pod-0', scheduled=True),
            _pod('pod-1', scheduled=True)])
        condition = _condition(instance.PARK_MODE_SCHEDULING,
                               pods=('pod-0', 'pod-1'))
        assert condition._probe() == (
            False, 'Waiting for pods to be scheduled: 0/3 nodes are available')
        assert condition._probe()[0] is True

    @pytest.mark.parametrize(
        'mode', [instance.PARK_MODE_ADMISSION, instance.PARK_MODE_SCHEDULING])
    def test_resumes_when_expected_pod_missing(self, monkeypatch, mode):
        _serve(monkeypatch, [_pod('pod-1', gated=True)])
        condition = _condition(mode, pods=('pod-0', 'pod-1'))
        assert condition._probe()[0] is True

    @pytest.mark.parametrize(
        'mode', [instance.PARK_MODE_ADMISSION, instance.PARK_MODE_SCHEDULING])
    def test_resumes_when_pod_has_deletion_timestamp(self, monkeypatch, mode):
        _serve(monkeypatch, [_pod('pod-0', gated=True, deleting=True)])
        condition = _condition(mode)
        assert condition._probe()[0] is True

    def test_resumes_at_deadline(self, monkeypatch):
        core_api = _serve(monkeypatch, [_pod('pod-0', gated=True)])
        condition = _condition(instance.PARK_MODE_ADMISSION, deadline=100.0)
        monkeypatch.setattr(instance.time, 'time', lambda: 99.0)
        assert condition._probe()[0] is False
        monkeypatch.setattr(instance.time, 'time', lambda: 101.0)
        assert condition._probe()[0] is True
        # Past the deadline the answer is known without asking the cluster.
        assert core_api.list_namespaced_pod.call_count == 1

    @pytest.mark.parametrize('error', [
        urllib3.exceptions.MaxRetryError(None, '/api/v1/pods'),
        instance.kubernetes.api_exception()(status=500, reason='boom'),
        instance.kubernetes.api_exception()(status=403, reason='Forbidden'),
    ])
    def test_api_error_is_a_missed_poll(self, monkeypatch, error):
        _serve(monkeypatch, error)
        condition = _condition(instance.PARK_MODE_ADMISSION)
        assert condition._probe() == (False, None)


class TestWait:

    @pytest.mark.asyncio
    async def test_wait_async_returns_false_when_cancelled(
            self, monkeypatch, no_sleep):
        core_api = _serve(monkeypatch, [_pod('pod-0', gated=True)])

        async def is_cancelled():
            return True

        result = await _condition(instance.PARK_MODE_ADMISSION
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

        result = await _condition(instance.PARK_MODE_ADMISSION
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

        result = await _condition(instance.PARK_MODE_ADMISSION
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

        result = await _condition(instance.PARK_MODE_SCHEDULING).wait_async(
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

        assert await _condition(instance.PARK_MODE_SCHEDULING).wait_async(
            is_cancelled=is_cancelled, fallback_wait_seconds=30) is True

    def test_wait_and_wait_async_agree(self, monkeypatch, no_sleep):
        timeline = ([_pod('pod-0', unschedulable_msg='a')
                    ], [_pod('pod-0', unschedulable_msg='b')],
                    [_pod('pod-0', scheduled=True)])

        sync_api = _serve(monkeypatch, *timeline)
        sync_reasons = []
        sync_result = _condition(instance.PARK_MODE_SCHEDULING).wait(
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
            _condition(instance.PARK_MODE_SCHEDULING).wait_async(
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
        assert _condition(instance.PARK_MODE_ADMISSION).wait(
            is_cancelled=lambda: next(answers),
            fallback_wait_seconds=30) is False


def test_condition_pickles():
    """The condition rides on ExecutionPausedError across the executor's
    process boundary."""
    condition = _condition(instance.PARK_MODE_SCHEDULING,
                           pods=('pod-0', 'pod-1'),
                           deadline=123.5)
    restored = pickle.loads(pickle.dumps(condition))
    assert type(restored) is instance.KubernetesPodWaitCondition
    assert restored.__dict__ == condition.__dict__


def test_condition_is_async_capable():
    """The executor uses wait_async (a coroutine on its shared loop, no OS
    thread per parked request) only when it is a coroutine function."""
    assert asyncio.iscoroutinefunction(
        instance.KubernetesPodWaitCondition.wait_async)


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
