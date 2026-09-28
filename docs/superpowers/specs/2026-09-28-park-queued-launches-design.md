# Queued launches must not hold scarce resources

Date: 2026-09-28
Status: draft, awaiting review
Branch: `jobs/park-queued-launches` (worktree `~/GitHub/skypilot-park-queued`), based on upstream master `61f1dc405`
Related: PLT-3594, PLT-3425, PLT-3598, PLT-3599, PLT-3432 item 2; upstream draft PRs #10881, #10882

## 1. Problem

A launch that is only *waiting* holds two resources that are sized for work being done, not for waiting:

| Wall | Resource | Size (prod) | Held by | Incident |
| -- | -- | -- | -- | -- |
| 1 | API-server long worker (a process in the executor pool) | 64 | a `sky.launch` request whose pods wait for Kueue admission or for scheduling | PLT-3425, 2026-09-18: every interactive `sky launch` hung |
| 2 | Jobs-controller launch slot (`ControllerManager.starting`, `LAUNCHES_PER_WORKER` = 8 per controller) | 176 | the same managed-job launches, plus every pool job waiting for a pool worker | PLT-3594, 2026-09-25: no non-pool managed job claimed for 16 h |

Waits of hours are normal under Kueue, so a fixed pool of workers or slots becomes the real capacity limit instead of the cluster.

## 2. What upstream already has (verified on `61f1dc405`)

Upstream built the "park and resume" framework as "a first step" (#9875, "Support execution wait on external condition", citing "admission or quota approval"), but never connected it to queue waits:

* `exceptions.ExecutionPausedError(message, hint, retry_wait_seconds, continue_condition)` (`sky/exceptions.py:755`). The executor catches it, sets the request to `WAITING`, frees the worker process, runs the condition's `wait_async` on a shared event loop, and requeues the request when it returns True (`sky/server/requests/executor.py:462-519`).
* Teardown layers re-raise it without cleaning up: `provisioner.bulk_provision` (`sky/provision/provisioner.py:301,352`) and `RetryingVmProvisioner._retry_zones` (`sky/backends/cloud_vm_ray_backend.py:1344`).
* Re-entry after resume is safe for Kubernetes: `_create_pods` counts existing Pending pods and creates only the missing ones (`to_start_count`, `sky/provision/kubernetes/instance.py:2417-2430`), and the `INSTANCES_REQUESTED` launch milestone is write-once "so a resumed launch re-entering here keeps the original time" (`instance.py:2316-2323`).
* The backend saves the cluster handle with `ready=False` (status INIT) before provisioning (`cloud_vm_ray_backend.py:1251`), so a resumed `sky.launch` finds an INIT cluster and relaunches on the same resources instead of re-optimizing.
* Jobs controller: `_await_launch_request` polls the inner request every 30 s. On `WAITING` it raises `_LaunchRequestParked`, which exits `scheduler.scheduled_launch`, whose `finally` removes the job from `starting`, releasing the slot. The task shows PENDING with the park reason. `_wait_for_parked_request` waits for the request to leave `WAITING`, then re-acquires a slot and re-attaches to the same request (`sky/jobs/recovery_strategy.py:795-913, 1411-1440`).
* The one open-source raiser today is cluster-lock contention (`cloud_vm_ray_backend.py:3478`, `locks.LockAcquirableCondition`).

**Gap:** `_wait_for_pods_to_schedule` (`instance.py:1275-1310`) handles Kueue-gated pods by calling `time.sleep(1)` in a loop inside the worker, for up to `kubernetes.kueue.admission_timeout` (default 24 h). Nothing raises `ExecutionPausedError` there, so both walls stay held.

**Separately:** pool jobs never call `sky.launch`, but `ControllerManager.start_job` puts every claimed job in `starting` (`sky/jobs/controller.py:3770`), and a pool job is only removed once it gets a worker (`controller.py:848`). `scheduled_launch` returns immediately for pool jobs (`sky/jobs/scheduler.py:421`), so the park path above never reaches them.

## 3. Goals and non-goals

Goals
1. A launch whose pods are held by a Kueue scheduling gate releases its API-server worker, and with it the jobs-controller launch slot, for the whole admission wait, then resumes automatically.
2. A launch with `provision_timeout: -1` whose pods are Unschedulable (with no gate) releases both too.
3. A pool job waiting for a worker holds no launch slot.
4. A pool job waiting for a worker survives an API-server or controller restart (today: `FAILED_CONTROLLER` from `assert cluster_name is not None`).
5. No change in behavior for launches that schedule promptly, for finite `provision_timeout` failover, or for anything outside an API-server request context.

Non-goals
* The per-user launch cap (#10882). It stays a separate PR. Note for its rebase: a parked job keeps `schedule_state = LAUNCHING` (§4.5), so #10882's per-user LAUNCHING count must exclude parked jobs, or it will throttle users for jobs that hold nothing.
* Parking on clouds other than Kubernetes, and parking a finite-`provision_timeout` wait.
* Idle-first pool scale-down (PLT-3595) and the monitor-loop retry backport (#10040).

## 4. Design

### 4.1 `KubernetesPodWaitCondition` (new, `sky/provision/kubernetes/pod_wait_condition.py`)

A `ContinueCondition` defining both `wait()` and `wait_async()`, following `locks.LockAcquirableCondition`. It holds only picklable state: `context`, `namespace`, `cluster_name_on_cloud`, `expected_pod_names`, `mode` (`'admission'` or `'scheduling'`), `deadline` (absolute epoch seconds, or None), and `poll_seconds` (default 10).

On each poll it lists the pods with the cluster label (same selector and request timeout as `_wait_for_pods_to_schedule`) and returns:

* **True (resume)** when any of the following hold. In every case the resumed provisioning attempt re-evaluates and handles the outcome with the existing code, so error reporting stays in one place.
  * `admission`: no expected pod has scheduling gates left (admitted).
  * `scheduling`: every expected pod is scheduled (`_pod_is_scheduled`).
  * either mode: any expected pod is missing or has a deletion timestamp. The resumed attempt raises the existing `_deleted_pods_error` with the real reason.
  * either mode: `deadline` has passed. The resumed attempt raises the existing admission-timeout error.
* **False (drop)** when `is_cancelled()` is true.
* **Keep waiting** otherwise. In `scheduling` mode, call `update_status_msg` whenever the scheduler's `Unschedulable` condition message changes. In `admission` mode the message stays fixed (`Waiting for queue admission`). Open source has no queue-position text to show: `_update_spinner_message` is an empty stub (`instance.py:1037-1043`), apparently another hook the platform fills. Reading the Kueue Workload's status for a richer message would need RBAC that SkyPilot doesn't grant today, so it is left out.
* A transport or API error on a poll is logged and treated as a missed poll. The executor already falls back to "reschedule" if `wait_async` raises, so a persistent error resumes the request, which then surfaces the error through the normal provisioning path.

### 4.2 Raise sites in `_wait_for_pods_to_schedule`

Both are gated on `common_utils.is_in_request_context()` (same as the lock pause) and on `kubernetes.park_queued_launches` (§4.4).

* **Admission.** In the existing gated branch, once the pods have been gated for `_PARK_AFTER_GATED_SECONDS` (30 s), raise `ExecutionPausedError('Waiting for queue admission', hint=..., retry_wait_seconds=30, continue_condition=KubernetesPodWaitCondition(mode='admission', deadline=...))`. The grace period means a job admitted within seconds never pays a park/resume round trip. The existing spinner update and `LAUNCH_PROGRESS` cluster event still run before the raise.
* **Scheduling.** When `timeout < 0`, no pod is gated, the volume probe reports nothing pending, and every unscheduled expected pod has had `PodScheduled=False, reason=Unschedulable` continuously for `_PARK_AFTER_UNSCHEDULABLE_SECONDS` (60 s), raise the same error with `mode='scheduling'`, `deadline=None`. Volume waits and autoscaler scale-ups stay in the worker: they have their own diagnostics and deadlines.

### 4.3 The admission deadline must survive a park

Today the gated wait's clock is `start_time = time.time()` at function entry, so every resume would restart the 24 h budget. Change it to the earliest `metadata.creation_timestamp` of the expected pods. Pods persist across a park, so that is stable and needs no extra state. It also gives the condition its absolute `deadline`. For admission timeouts that are not parked (outside a request context), this changes nothing except that the few seconds between pod creation and the wait loop now count.

### 4.4 Configuration

`kubernetes.park_queued_launches` (bool, default `true`, per-context override like the other `kubernetes.*` keys), added to the schema in `sky/utils/schemas.py` and to the config reference docs. `false` restores today's in-worker wait. Default on because upstream built the framework for exactly this and the user-visible difference is small (§4.6). This is the one choice here most likely to change in upstream review.

### 4.5 Managed-jobs controller changes

* **#10881 (rebased, unchanged in substance):** `start_job` adds to `starting` only when `pool is None`.
* **HA resume for pool jobs:** in `_run_one_task`, when `is_resume`, the job has a pool, `get_pool_submit_info_async` returns no cluster name, and the status is not CANCELLED/CANCELLING or terminal, treat the task as never started: call `self._strategy_executor.launch()`, re-read the submit info, `set_started_async`, and continue into monitoring. This replaces reaching `assert cluster_name is not None`. It applies only to pool jobs. A non-pool job always has a generated cluster name, so it is unaffected.
* **No change for non-pool parking:** the existing `_LaunchRequestParked` path already releases and re-acquires the slot. A parked job keeps `schedule_state = LAUNCHING`. On master nothing counts LAUNCHING rows (`get_num_launching_jobs` has no callers, and the claim query has no priority blocking), so this is harmless today. It matters only for #10882 (see §3).

### 4.6 User-visible behavior

* `sky launch` (interactive) on a queued cluster: the request becomes `WAITING` with status message `Waiting for queue admission (waiting to resume)`, which the server pushes to the client as the live status (`sky/server/stream_utils.py:475-487`). The client's log stream stays open with heartbeats (`sky/server/stream_utils.py:475`). When admitted, provisioning continues in the same stream.
* `sky jobs queue`: the task shows PENDING with the reason `Job is waiting to launch: Waiting for queue admission ...` instead of STARTING.
* `sky api status`: parked requests show as WAITING and no longer count against running long workers.

## 5. Risks and how each is tested (none assumed)

| # | Risk | Test |
| -- | -- | -- |
| R1 | A resumed `sky.launch` re-optimizes onto a different context, namespace, or cloud | e2e: two kind contexts, park on one, resume, and assert the pods and handle are on the original context |
| R2 | Resume creates duplicate pods or a second Kueue workload | e2e: pod and workload count before and after resume. Unit: `_create_pods` re-entry with gated Pending pods |
| R3 | The status refresher or `sky status --refresh` acts on the INIT cluster while its lock is released during the park (e.g. marks it abnormal or tears it down) | e2e: park, run `sky status --refresh` repeatedly and wait past the refresh interval, assert the pods survive and the cluster stays INIT |
| R4 | `sky down`, `sky api cancel` or `sky jobs cancel` during a park leaves orphaned pods or a zombie request | e2e: each cancel path while parked; assert the pods are gone, the request is CANCELLED, and the job is CANCELLED |
| R5 | API-server restart while parked loses the request or double-launches | e2e (consolidation mode): restart while parked; assert the job resumes onto the same pods, exactly one of them, and SUCCEEDS |
| R6 | The admission deadline resets on resume | unit (deadline from pod creation) and e2e with `admission_timeout: 120`: fails at about 120 s after pod creation with the admission error |
| R7 | Kueue evicts or deletes pods while parked, and the error is lost | e2e: delete the workload while parked; assert the job fails or recovers with the deletion reason |
| R8 | Fast admissions get slower | e2e: queue with free quota, compare launch latency with patched and stock images over 5 runs each; assert no park and a comparable median |
| R9 | A finite-timeout Unschedulable launch changes behavior | e2e: `provision_timeout: 60` with an unsatisfiable nodeSelector; fails over exactly as on stock |
| R10 | Old client with new server, or new client with old server | e2e: client from `61f1dc405` (stock) against the patched server and vice versa, for `sky launch` and `sky jobs launch` on a queued context |
| R11 | Pickling or the executor boundary breaks the condition | unit: pickle round-trip; executor test that drives `wait_async` to resume |
| R12 | Pool slot and HA fixes regress plain jobs | unit tests from #10881, plus new HA-resume tests; e2e: 1-worker pool with 20 jobs plus a plain job, then a restart with pool jobs waiting |

## 6. Test plan

**Unit** (`tests/unit_tests`, run with the repo's pytest config):
* `test_sky/provision/kubernetes/test_pod_wait_condition.py` (new):
  * admitted, scheduled, deleted, deadline, cancelled, transient error, and status-message refresh;
  * `wait` and `wait_async` parity;
  * pickling.
* `test_sky/provision/kubernetes/test_instance.py` (extend):
  * park after the gate grace, not before;
  * no park outside a request context or with the config off;
  * park on Unschedulable only when `timeout < 0` and after the grace;
  * no park while a volume or autoscaler wait is in progress;
  * the deadline is derived from pod creation;
  * re-entry creates no pods.
* `test_sky/jobs/test_controller.py`: #10881 tests, plus HA resume of a pool job with no submit info (launches rather than asserting; the cancelled case still cancels).
* Existing suites stay green: `test_provisioner_pause.py`, `test_executor.py`, `test_recovery_strategy.py`, `test_locks.py`, `test_cloud_vm_ray_backend.py`, and the rest of `tests/unit_tests` (full run before commit).
* `bash format.sh` (yapf, isort, mypy, pylint at pinned versions).

**End-to-end** (local, isolated; never touches the `metamorphic.teleport.sh-*` contexts):
* kind v0.33.0 clusters with Kueue v0.19.6 (pod integration on), a ClusterQueue with adjustable CPU quota, and a LocalQueue in the SkyPilot namespace. A second kind cluster for R1.
* A dedicated `HOME` and `KUBECONFIG` in the scratchpad, and a local API server from the worktree. The local server caps long workers at 4, which makes wall 1 reproducible with 4 queued jobs. Consolidation mode enabled for the managed-jobs scenarios.
* Each scenario runs first on stock `61f1dc405` to reproduce the failure, then on the patched branch. Evidence per scenario: `sky api status`, `sky jobs queue`, `kubectl get pods,workloads`, and the `/metrics` gauges (`sky_managed_jobs_controller_starting_count`, long-executor gauge).
* Core scenario: set quota to 0. Submit 6 managed jobs and 2 interactive `sky launch`es. Stock: the workers and slots saturate and a trivial job on a second, unqueued context hangs. Patched: requests are WAITING, the worker gauge drops to free, and the trivial job completes. Raise the quota: all queued jobs resume and SUCCEED with exactly one pod set each.
* Then R1 to R12 as listed in §5.

**Staging (optional, your call):** the `skypilot-test` instance, with an image built from this branch, repeating the core scenario on real Kueue.

## 7. Rollout (after upstream review, not part of this branch)

Build an image from the 0.13.0 backport or the next upstream release that includes it. Roll out in a quiet window: until the HA-resume fix is in the image, don't restart while pool jobs are STARTING. Verify with the fleet-alloc "Controller launch slots in use" stat and the long-executor gauge.

## 8. Deliverables

1. Commits on `jobs/park-queued-launches`:
   (a) the admission park plus condition, deadline and config;
   (b) the Unschedulable park;
   (c) #10881 pool slot;
   (d) the HA resume.
   (b) is kept separate so it can be dropped. (c) and (d) may go upstream as their own PR.
2. A test report with the stock versus patched evidence for every scenario in §5 and §6.
3. PR descriptions (drafts, not opened without your go-ahead).
