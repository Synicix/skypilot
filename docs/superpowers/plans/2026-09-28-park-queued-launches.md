# Park Queued Launches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A launch that is only waiting (on Kueue admission, or Unschedulable under `provision_timeout: -1`) gives back its API-server worker and its jobs-controller launch slot. Pool jobs never hold a slot and survive a restart.

**Architecture:** Raise upstream's existing `ExecutionPausedError` from `_wait_for_pods_to_schedule` with a new picklable `KubernetesPodWaitCondition`. Everything else (WAITING state, freed worker, requeue, the controller releasing its slot) already exists upstream. Plus two small controller fixes.

**Tech Stack:** Python 3.11, pytest (with the repo's `pyproject.toml` addopts), kubernetes client, kind v0.33.0 + Kueue v0.19.6 for e2e.

**Spec:** `docs/superpowers/specs/2026-09-28-park-queued-launches-design.md`

## Global Constraints

- Park only when `common_utils.is_in_request_context()` is true and `kubernetes.park_queued_launches` is not false (default `true`, resolved with `skypilot_config.get_effective_region_config(cloud='kubernetes', region=context, keys=('park_queued_launches',), default_value=True)`; for SSH node pools, use `cloud='ssh'` the same way the function already does).
- Grace periods: `_PARK_AFTER_GATED_SECONDS = 30`, `_PARK_AFTER_UNSCHEDULABLE_SECONDS = 60`. Condition poll interval: `10` s.
- Error `retry_wait_seconds = 30`. Messages: `'Waiting for queue admission'` and `'Waiting for pods to be scheduled'`.
- Unschedulable parking only when `timeout < 0`.
- The admission deadline is anchored at the earliest real `datetime` `metadata.creation_timestamp` among the expected listed pods, falling back to function-entry `time.time()`.
- The condition class lives in `sky/provision/kubernetes/instance.py`. It must pickle, and must not import anything that `instance.py` imports lazily.
- Style: yapf 0.32 / pylint 2.14.5 / mypy 1.19.1 through `bash format.sh --files ...`. Comment density matches `instance.py`. TODOs as `TODO(synicix)`.
- Tests run with `$VENV/bin/python -m pytest` where `VENV=/tmp/claude-1000/-home-synicix-GitHub-skypilot/2d056b44-7c10-401e-98c1-c81e0e103c85/scratchpad/venv`, reinstalled editable from the worktree (Task 0).
- Never use the `metamorphic.teleport.sh-*`, `at` or `morpho` kube contexts. E2E runs with `HOME` and `KUBECONFIG` inside the scratchpad.

## Review Focus

1. The gate comes off between park and first poll: the condition must resume on its first probe, not wait out a full interval (test in Task 1).
2. Multi-node launch where only some pods are gated: the existing loop already treats "any expected pod gated" as the gated state, so parking follows it (a partially admitted pod group is still a queue wait); resume only when none is gated (tests in Tasks 1 and 2).
3. Pods deleted while parked (Kueue eviction, `sky down`): the condition resumes and the resumed attempt raises the existing deleted-pods error (tests in Tasks 1 and 2, e2e R4/R7).
4. A launch not running in a request context (e.g. the SDK used in-process, or the non-API path) must never raise the pause, or it becomes an unhandled exception (test in Task 2).
5. A cancelled request while parked: `wait_async` returns False promptly and leaves no reschedule (test in Task 1, e2e R4).

---

### Task 0: Worktree venv

- [ ] `VIRTUAL_ENV=$VENV uv pip install -e ".[kubernetes]"` from the worktree root. Check that `$VENV/bin/python -c "import sky; print(sky.__file__)"` prints a path under `skypilot-park-queued`.
- [ ] Baseline: `$VENV/bin/python -m pytest tests/unit_tests/kubernetes/test_provision.py tests/unit_tests/test_sky/provision/test_provisioner_pause.py tests/unit_tests/test_sky/server/requests/test_executor.py tests/unit_tests/test_sky/jobs/test_controller.py tests/unit_tests/test_sky/jobs/test_recovery_strategy.py`. Record the pass/fail counts; any failure that already exists is noted, not fixed.

### Task 1: `KubernetesPodWaitCondition`

**Files:**
- Modify: `sky/provision/kubernetes/instance.py` (new class after `_pod_is_scheduled`, ~line 272)
- Test: `tests/unit_tests/kubernetes/test_pod_wait_condition.py` (new)

**Interfaces:**
- Produces:
  - `class KubernetesPodWaitCondition` with `__init__(self, *, context: Optional[str], namespace: str, cluster_name_on_cloud: str, expected_pod_names: List[str], mode: str, deadline: Optional[float], poll_seconds: float = 10.0)`. `mode` is `'admission'` or `'scheduling'`, from module constants `PARK_MODE_ADMISSION` and `PARK_MODE_SCHEDULING`.
  - `_probe(self) -> Tuple[bool, Optional[str]]` returns (resume?, fresh reason or None).
  - `wait(self, *, is_cancelled, fallback_wait_seconds, update_status_msg=None) -> bool`
  - `async wait_async(self, *, is_cancelled, fallback_wait_seconds, update_status_msg=None) -> bool` (the callables are async; the blocking probe goes through `loop.run_in_executor(None, self._probe)`).

- [ ] **Step 1: Write the failing tests** (mock `sky.adaptors.kubernetes.core_api`; fake pods as in `test_provision.py`'s `_make_pending_pod`/`_add_gate`, copied into the new file):
  - `test_admission_resumes_when_no_pod_gated` (pod-0 gated then ungated → `_probe()` gives `(False, …)` then `(True, …)`)
  - `test_admission_waits_while_any_pod_gated` (2 pods, one gated → no resume)
  - `test_scheduling_resumes_when_all_scheduled` (1 of 2 bound → no; both bound → yes)
  - `test_resumes_when_expected_pod_missing` and `test_resumes_when_pod_has_deletion_timestamp`
  - `test_resumes_at_deadline` (`deadline=100`, patched `time.time` → 101 → resume)
  - `test_transport_error_is_a_missed_poll` (`list_namespaced_pod` raises `urllib3` `MaxRetryError` → `(False, None)`, no raise)
  - `test_wait_async_returns_false_when_cancelled` (async `is_cancelled` returns True → returns False, zero probes after it)
  - `test_wait_async_first_probe_is_immediate` (ungated from the start → returns True with `asyncio.sleep` never awaited; patch `asyncio.sleep` to record calls)
  - `test_scheduling_reason_refreshed_on_change` (Unschedulable message 'a' → 'a' → 'b'; `update_status_msg` awaited exactly with `['a', 'b']`)
  - `test_wait_and_wait_async_agree` (same pod timeline → same return value and probe count)
  - `test_condition_pickles` (`pickle.loads(pickle.dumps(c)).__dict__ == c.__dict__`)
- [ ] **Step 2:** run `$VENV/bin/python -m pytest tests/unit_tests/kubernetes/test_pod_wait_condition.py`. Expect: FAIL, `AttributeError: ... KubernetesPodWaitCondition`.
- [ ] **Step 3: Implement.** The listing is the same as `_wait_for_pods_to_schedule`'s (the cluster label selector, `_POD_POLL_REQUEST_TIMEOUT`). Transport errors are classified with `_is_transport_error`; any other `ApiException` also counts as a missed poll (logged at debug), because the executor's fallback reschedules if it escapes anyway. The scheduling-mode reason is the `message` of the `PodScheduled=False` condition on the first unscheduled pod. The loop probes first, then sleeps `poll_seconds`.
- [ ] **Step 4:** tests PASS.
- [ ] **Step 5:** commit `[Kubernetes] Add KubernetesPodWaitCondition for parked launches`.

### Task 2: Park on queue admission; deadline from pod creation; config key

**Files:**
- Modify: `sky/provision/kubernetes/instance.py` (`_wait_for_pods_to_schedule`, lines ~1085-1470)
- Modify: `sky/utils/schemas.py` (`_CONTEXT_CONFIG_SCHEMA_KUBERNETES`, next to `'kueue'`, ~line 1723), `docs/source/reference/config.rst` (new `kubernetes.park_queued_launches` section after `kubernetes.kueue.admission_timeout`, ~line 2036)
- Test: `tests/unit_tests/kubernetes/test_provision.py` (new class `TestWaitForPodsToScheduleParking`, reusing the `TestWaitForPodsToScheduleQueueGating` harness; request context via `monkeypatch.setattr(instance.common_utils, 'is_in_request_context', lambda: True)`)

**Interfaces:**
- Consumes: Task 1 `KubernetesPodWaitCondition`, `PARK_MODE_ADMISSION`.
- Produces: `_park_allowed(context: Optional[str], is_ssh_node_pool: bool) -> bool`; `_gated_wait_anchor(pods: List[Any], expected: Set[str], fallback: float) -> float`.

- [ ] **Step 1: Failing tests:**
  - `test_parks_after_gate_grace`: gated forever, request context → raises `ExecutionPausedError`; `clock.now` between 30 and 32; `err.continue_condition.mode == 'admission'`; `err.retry_wait_seconds == 30`; the queue-admission `LAUNCH_PROGRESS` event was emitted before the raise.
  - `test_no_park_when_admitted_within_grace`: gated 0-10 s, then Running → returns normally, no raise.
  - `test_no_park_outside_request_context`: `is_in_request_context` is False, `admission_timeout=100` → the existing `KubernetesError('...scheduling gates...')` at about 100 s.
  - `test_no_park_when_config_disabled`: config `('park_queued_launches',)` → False → same as the previous test.
  - `test_park_deadline_anchored_at_pod_creation`: pod `creation_timestamp` = epoch 0 as a real `datetime`, fake clock starting at 50, `admission_timeout=100` → `err.continue_condition.deadline == 100`.
  - `test_resume_after_deadline_raises_admission_error`: the same pod on a second call with the clock at 101 → `KubernetesError` 'scheduling gates' immediately (re-entry honours the original deadline).
  - `test_admission_timeout_negative_gives_no_deadline`: `admission_timeout=-1` → `deadline is None`.
  - `test_partially_gated_group_parks` (2 pods: one gated, one ungated-unscheduled → parks); `test_deleted_pod_during_gate_still_errors` (the existing deleted-pods path wins over parking).
  - The existing `TestWaitForPodsToScheduleQueueGating` tests stay green unchanged (they run without a request context).
- [ ] **Step 2:** run the new class → FAIL (no raise).
- [ ] **Step 3: Implement.**
  - Compute `gated_wait_anchor` on the first successful poll with `_gated_wait_anchor`, using `_utc` only on `datetime` instances. `_evaluate_timeout` uses it in place of `start_time` for the gated bound.
  - Record `gated_since = time.time()` on entering the gated state.
  - In the gated branch, after the spinner and event, `if _park_allowed(...) and time.time() - gated_since >= _PARK_AFTER_GATED_SECONDS:` raise `exceptions.ExecutionPausedError('Waiting for queue admission', hint=..., retry_wait_seconds=30, continue_condition=KubernetesPodWaitCondition(mode=PARK_MODE_ADMISSION, deadline=None if admission_timeout < 0 else anchor + admission_timeout, ...))`.
  - The hint is: ``'The launch resumes automatically once the queue admits its pods. Check the queue: `kubectl describe workload -n <namespace>`.'``
  - Admission parking does not depend on `provision_timeout`: that clock only starts at admission.
  - `run_instances` already lets non-Kubernetes errors through. Verify that `ExecutionPausedError` is not caught by `run_instances`' `except (kubernetes.api_exception(), config_lib.KubernetesError)` (it isn't; add a test `test_run_instances_propagates_pause`).
  - Schema: `'park_queued_launches': {'type': 'boolean'}`. Docs section explaining WAITING and freed workers, default `true`.
- [ ] **Step 4:** the new class, `TestWaitForPodsToScheduleQueueGating` and `tests/unit_tests/test_sky/utils/test_schemas*.py` (if present) all PASS.
- [ ] **Step 5:** commit `[Kubernetes] Park launches waiting for queue admission instead of holding a worker`.

### Task 3: Park Unschedulable pods when `provision_timeout < 0`

**Files:** same `instance.py` function; tests in `TestWaitForPodsToScheduleParking`.

**Interfaces:** Consumes `PARK_MODE_SCHEDULING`. Produces `_all_unschedulable(pods: List[Any]) -> bool` (true when each pod has `PodScheduled` status `False` with reason `Unschedulable`).

- [ ] **Step 1: Failing tests:**
  - `test_parks_unschedulable_with_infinite_timeout` (timeout=-1, Unschedulable since t=0 → raises at t≈60, `mode == 'scheduling'`, `deadline is None`)
  - `test_no_park_unschedulable_with_finite_timeout` (timeout=120 → existing `_raise_pod_scheduling_errors` path at about 120 s)
  - `test_unschedulable_streak_resets` (Unschedulable 0-40, reason flips to none 40-50, Unschedulable again → park at ≥110, not at 60)
  - `test_no_park_while_volume_pending` (`_PendingVolumeProbe.probe` patched to return a message → no park by t=300 with timeout=-1; stop the loop by making the pod Running at 300)
  - `test_no_park_while_scale_up_in_flight` (autoscaler set, `_cluster_had_autoscale_event` True → no park within `_AUTOSCALE_DETECTED_TIMEOUT_SECONDS`)
- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** in the non-gated section, after `volume_wait_msg` is computed: track `unschedulable_since` (reset whenever `_all_unschedulable(unscheduled_pods)` is false, or `volume_wait_msg` is set, or `scale_up_in_flight`). Park when `timeout < 0 and _park_allowed(...) and now - unschedulable_since >= _PARK_AFTER_UNSCHEDULABLE_SECONDS`.
- [ ] **Step 4:** PASS, including every existing `TestWaitForPodsToSchedule*` class.
- [ ] **Step 5:** commit `[Kubernetes] Park unschedulable launches when provision_timeout is -1` (kept separate so it can be dropped).

### Task 4: Pool jobs hold no launch slot (#10881)

**Files:** `sky/jobs/controller.py:3770` (`start_job`), `tests/unit_tests/test_sky/jobs/test_controller.py` (class `TestStartJobLaunchSlots`).

- [ ] `git fetch upstream pull/10881/head:pr-10881`, then `git cherry-pick` its commit(s). Resolve conflicts if any.
- [ ] Run `TestStartJobLaunchSlots`: PASS. Revert the source hunk locally and confirm that 2 of its 3 tests FAIL, then restore.
- [ ] Commit (the cherry-pick keeps its authorship; add a `Co-Authored-By` line only if edited).

### Task 5: HA resume of a pool job still waiting for a worker

**Files:** `sky/jobs/controller.py` `_run_one_task` (~lines 815-845), `tests/unit_tests/test_sky/jobs/test_controller.py`.

**Interfaces:** Consumes `StrategyExecutor.launch() -> float`, `managed_job_state.get_pool_submit_info_async(job_id) -> Tuple[Optional[str], Optional[int]]`.

- [ ] **Step 1: Failing tests** (use the existing `_run_one_task` fixtures in `test_controller.py`; find them with `grep -n "is_resume=True" tests/unit_tests/test_sky/jobs/test_controller.py`):
  - `test_resume_pool_job_without_worker_launches`: pool set, `is_resume=True`, the first `get_pool_submit_info_async` returns `(None, None)` and the second `('pool-worker-1', 7)`, status STARTING → `launch` awaited once, `set_started_async` awaited once, monitoring proceeds with `cluster_name == 'pool-worker-1'`, no `AssertionError`.
  - `test_resume_pool_job_cancelled_while_waiting`: status CANCELLED → `asyncio.CancelledError`, `launch` not awaited.
  - `test_resume_pool_job_with_worker_unchanged`: submit info present → `launch` not awaited (the current behavior is kept).
- [ ] **Step 2:** FAIL (the first test raises `AssertionError`).
- [ ] **Step 3: Implement** right after the pool submit-info read. If `is_resume and self._pool and cluster_name is None`, check the status (the existing cancelled check). If not cancelled, `remote_job_submitted_at = await self._strategy_executor.launch()`, re-read the submit info, and `await managed_job_state.set_started_async(...)` exactly as the `not is_resume` branch does. Then continue. Keep the assert for every other path.
- [ ] **Step 4:** PASS, and the whole `test_controller.py` PASS.
- [ ] **Step 5:** commit `[Jobs] Relaunch a resumed pool job that was still waiting for a worker`.

### Task 6: Full unit suite and format

- [ ] `bash format.sh --files <every changed file>`. Expect no diff afterwards, and mypy/pylint clean for the changed files.
- [ ] `$VENV/bin/python -m pytest tests/unit_tests/`: compare against the Task 0 baseline. There must be no new failures.
- [ ] Commit any formatting fix-ups.

### Task 7: E2E on kind + Kueue (stock vs patched)

**Files (scratchpad only, not committed):** `$S/e2e/kind-*.yaml`, `$S/e2e/kueue-*.yaml`, `$S/e2e/run_*.sh`, `$S/e2e/report.md`.

- [ ] Install kind v0.33.0 to `$S/bin/kind`. `export HOME=$S/e2e/home KUBECONFIG=$S/e2e/kubeconfig`, and check that `kubectl config get-contexts` lists only `kind-*`.
- [ ] Create `kind-q` (Kueue v0.19.6 manifests, pod integration enabled for namespace `skypilot-e2e`, a ClusterQueue with `cpu` nominal quota set by `kubectl patch`, a LocalQueue `lq`) and `kind-free` (no Kueue).
- [ ] Sky config: `allowed_contexts: [kind-kind-q, kind-kind-free]`, `kubernetes.context_configs.kind-kind-q.kueue.local_queue_name: lq`, `namespace: skypilot-e2e`, `jobs.controller.consolidation_mode: true`. API server started from a stock checkout (`git worktree add $S/stock 61f1dc405` and its own venv) for the stock runs and from this worktree for the patched runs.
- [ ] Scenarios. Each is run stock first, then patched. For each, record `sky api status -a`, `sky jobs queue`, `kubectl get pods,workloads -A`, and `curl :9090/metrics | grep -E 'long_executors|starting_count'` in the report:
  - Core: quota 0, 6 × `sky jobs launch` (1 CPU) plus 2 × `sky launch -d` on kind-q, then `sky launch` of `echo ok` on kind-free. Expected: stock hangs, patched completes. Then quota 16: all resume and SUCCEED, with one pod per job (R2).
  - R1: the handle context after resume equals kind-kind-q.
  - R3: `sky status --refresh` ×5 while parked, and wait past 2 status-refresh intervals → the pods survive and the cluster stays INIT.
  - R4: `sky down` a parked cluster, `sky api cancel` a parked request, `sky jobs cancel` a parked job → pods gone, request CANCELLED, job CANCELLED.
  - R5: `sky api stop && sky api start` while parked → the job resumes onto the same pods and SUCCEEDS.
  - R6: `admission_timeout: 120` → FAILED with the admission error about 120 s after pod creation (± one poll).
  - R7: `kubectl delete workload` while parked → the job reports the deletion reason (recovers or fails according to its recovery policy).
  - R8: quota free; 5 launches each on stock and patched. Compare median time to UP; the patched server must show no WAITING.
  - R9 (on kind-free): nodeSelector that no node matches. With `provision_timeout: 60`, failure/failover identical to stock. With `-1`, parked (WAITING) and then resumed and UP after labelling the node.
  - R10: stock client with patched server, and patched client with stock server, for `sky launch` and `sky jobs launch` on kind-q under quota 0, then quota raised.
  - R12: pool of 1 worker on kind-free plus 20 pool jobs plus 1 plain job → the plain job is claimed in under 1 min (patched) or stuck (stock). Then `sky api stop/start` with pool jobs waiting → no FAILED_CONTROLLER (patched).
- [ ] Write `$S/e2e/report.md`. Any scenario that fails goes back to the owning task (systematic-debugging), and the fix gets its own unit test before the e2e run is repeated.

### Task 8: Hand-off

- [ ] Whole-branch review (superpowers:requesting-code-review).
- [ ] Draft the PR description(s) into the scratchpad. Do not push or open PRs without your go-ahead.
