# Runner-native `resources` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the per-job `resources` dict runner-specific: docker takes a flat map of `docker run` flags, Kubernetes keeps the verbatim Pod dict, and the shared Kubernetes quantity parser goes away.

**Architecture:** No schema or API change. `runner_config.validate_resources` shrinks to a mapping check; `docker_config.resource_flags` becomes a flat key→flag pass-through; Kubernetes code is untouched. Docs and docstrings follow.

**Tech Stack:** Python 3.12, pytest, ruff.

**Spec:** `docs/superpowers/specs/2026-11-07-runner-native-resources-design.md`

## Global Constraints

- All imports at the top of the file; no `Any` shortcuts (CLAUDE.md).
- No history comments; delete dead code outright.
- Docker key → flag is `f"--{key}"`; values are passed through as `str(value)`.
- Docs in `docs/` follow the `markdown-style` skill.

## Review Focus

- A docker value that is a dict or list (`{"memory": {"limit": 1}}`) must fail the task with an error naming the key, not emit `--memory {'limit': 1}` (Task 2).
- A docker key with leading dashes (`"--cpus"`) must be rejected, not become `----cpus` (Task 2).
- A docker key with an empty or space-containing name must be rejected (Task 2).
- A non-mapping `resources` (`["cpus"]`) is still rejected at submission (Task 1).
- `resources` on a subprocess job is still rejected at submission (existing test, Task 1 keeps it).

---

### Task 1: Submission validation becomes shape-only

**Files:**
- Modify: `aaiclick/orchestration/runner_config.py` (module docstring line 2; delete lines 115-162 except `validate_resources`; drop `Decimal` import if unused)
- Modify: `aaiclick/orchestration/registered_jobs.py:158-159`, `:381-383` (docstrings)
- Modify: `aaiclick/orchestration/kubernetes_config.py:13-14` (comment)
- Test: `aaiclick/orchestration/test_runner_config.py`, `aaiclick/orchestration/test_registered_jobs.py`

**Interfaces:**
- Produces: `validate_resources(resources: dict | None) -> None` raising `ValueError("resources must be a mapping, got …")` for a non-dict. `parse_quantity` no longer exists.

- [ ] **Step 1: Rewrite the tests**

In `test_runner_config.py` delete `test_parse_quantity`, `test_validate_resources_passes_other_keys_through` and the `parse_quantity` / `Decimal` imports. Replace `test_validate_resources_rejects` with:

```python
@pytest.mark.parametrize(
    "resources",
    [pytest.param([{"cpus": "1"}], id="list"), pytest.param("cpus=1", id="string")],
)
def test_validate_resources_rejects_non_mapping(resources):
    with pytest.raises(ValueError, match="resources must be a mapping"):
        validate_resources(resources)


def test_validate_resources_accepts_any_mapping():
    """Contents are the runner's business: docker flags or a Pod dict."""
    validate_resources({"cpus": "1.5", "memory": "512m"})
    validate_resources({"limits": {"cpu": "1", "nvidia.com/gpu": 1}})
```

In `test_registered_jobs.py` delete `test_run_job_rejects_malformed_resources` and the `malformed-quantity` param; change `resources-on-subprocess` to `{"resources": {"cpus": "1"}}`; change the three `{"limits": {"cpu": "1"}}` / `"2"` values in `test_register_job_persists_build_and_resources` and `test_run_job_resources_fall_back_to_registration` to `{"cpus": "1"}` / `{"cpus": "2"}`.

- [ ] **Step 2: Run to verify failures**

Run: `uv run pytest aaiclick/orchestration/test_runner_config.py -q`
Expected: FAIL — `test_validate_resources_accepts_any_mapping` raises "unknown resources section".

- [ ] **Step 3: Implement**

`validate_resources` keeps only the `None` return and the `isinstance(resources, dict)` check. Delete `_QUANTITY_SUFFIXES`, `_QUANTITY_RE`, `_RESOURCE_SECTIONS`, `parse_quantity`, and the `Decimal` import. Module docstring line 2: "plus the job's ``resources`` shape check". Docstring: "Reject a non-mapping ``resources`` at submission. Contents are runner-specific (docker flags or a Pod ``resources`` dict) and are checked by the worker at dispatch."

`registered_jobs.py` docstrings (both): "Runner-specific container resources: on docker a flat map of ``docker run`` flags (``{"cpus": "1.5", "memory": "512m"}``), on kubernetes the Pod container ``resources`` dict." Keep the "Rejected on a subprocess run" sentence.

`kubernetes_config.py:13-14`: "The one per-job setting, ``resources``, rides on ``JobDispatch`` and is runner-specific (docs/user_guide/orchestration.md "Resources")."

- [ ] **Step 4: Run**

Run: `uv run pytest aaiclick/orchestration/test_runner_config.py aaiclick/orchestration/test_registered_jobs.py -q && uv run ruff check aaiclick/orchestration`
Expected: PASS; ruff clean (also catches a now-unused `Decimal` import).

- [ ] **Step 5: Commit** — `Validate only the shape of resources at submission`

---

### Task 2: Docker `resources` is a flat flag map

**Files:**
- Modify: `aaiclick/orchestration/docker_config.py:309-329` (`resource_flags`, `_positive_limit`), import block line 30 (`parse_quantity`), `math` import if unused
- Modify: `aaiclick/orchestration/execution/docker_worker.py:92`, `:117` (docstrings mention "limits")
- Test: `aaiclick/orchestration/test_docker_config.py:217-247`

**Interfaces:**
- Consumes: nothing from Task 1 (`parse_quantity` import removed).
- Produces: `resource_flags(resources: dict | None) -> list[str]`; `ValueError` names `resources.<key>`.

- [ ] **Step 1: Rewrite the tests**

Replace both parametrized tests with:

```python
@pytest.mark.parametrize(
    "resources, expected",
    [
        pytest.param(None, [], id="none"),
        pytest.param({}, [], id="empty"),
        pytest.param({"cpus": "1.5", "memory": "512m"}, ["--cpus", "1.5", "--memory", "512m"], id="passthrough"),
        pytest.param({"cpu-shares": 512}, ["--cpu-shares", "512"], id="json-number"),
    ],
)
def test_resource_flags(resources, expected):
    assert resource_flags(resources) == expected


@pytest.mark.parametrize(
    "resources, match",
    [
        pytest.param({"memory": {"limit": 1}}, "resources.memory", id="nested-value"),
        pytest.param({"memory": None}, "resources.memory", id="null-value"),
        pytest.param({"--cpus": "1"}, "resources.--cpus", id="leading-dashes"),
        pytest.param({"": "1"}, "resources", id="empty-key"),
        pytest.param({"cpu shares": "1"}, "resources.cpu shares", id="space-in-key"),
    ],
)
def test_resource_flags_rejects(resources, match):
    with pytest.raises(ValueError, match=match):
        resource_flags(resources)
```

- [ ] **Step 2: Run to verify failures**

Run: `uv run pytest aaiclick/orchestration/test_docker_config.py -k resource_flags -q`
Expected: FAIL — `passthrough` raises "unknown"/ignores keys.

- [ ] **Step 3: Implement**

`resource_flags`: for each `key, value` in `(resources or {}).items()`, reject a key that is not `str`, is empty, starts with `-`, or contains whitespace, and a value that is not `str | int | float` (and not `bool`); emit `[f"--{key}", str(value)]`. One error message: `f"resources.{key} {value!r} is not a docker run flag value"` (key problems: `f"resources.{key!r} is not a docker run flag name"`). Delete `_positive_limit`, the `parse_quantity` import and `math` if unused. Docstring: "``docker run`` flags from a job's ``resources``: each ``key: value`` becomes ``--key value``, verbatim. Docker validates the values; its error fails the task."

`docker_worker.py` docstrings: "limits" → "resource flags".

- [ ] **Step 4: Run**

Run: `uv run pytest aaiclick/orchestration/test_docker_config.py aaiclick/orchestration/execution/test_docker_worker.py -q && uv run ruff check aaiclick/orchestration`
Expected: PASS.

- [ ] **Step 5: Commit** — `Docker resources: pass docker run flags through verbatim`

---

### Task 3: Docs

**Files:**
- Modify: `docs/user_guide/orchestration.md:444-461` ("Resources (docker / kubernetes)")
- Modify: `docs/designs/kubernetes_runner.md:261-267`
- Modify: `test_e2e/kubernetes/test_kubernetes_runner_e2e.py:242` (comment only if present; the k8s value stays valid)
- Delete: `docs/superpowers/specs/2026-11-07-runner-native-resources-design.md`, `docs/superpowers/plans/2026-11-07-runner-native-resources.md`

- [ ] **Step 1: User guide**

Rename the section `## Resources` and replace the table and prose with: one sentence that `resources` is a per-job dict whose shape is the deployment's runner (one runner per deployment; mixed fleets are not supported), then two blocks:

```python
# AAICLICK_RUNNER=docker — docker run flags, verbatim
run_job(name, image="python:3.12", resources={"cpus": "1.5", "memory": "512m"})

# AAICLICK_RUNNER=kubernetes — Pod container resources, verbatim
run_job(name, image="python:3.12", resources={"limits": {"cpu": "500m", "memory": "512Mi"}})
```

Close with: "A non-mapping is rejected at submission; a value the runner does not accept fails the task." Grep for `#resources-docker-kubernetes` anchors and update them to `#resources`.

- [ ] **Step 2: Design doc**

`kubernetes_runner.md:261-267`: replace "It rides on … (mapping: …)" with "Its shape is the deployment runner's: on kubernetes the Pod container `resources`, on docker a flat map of `docker run` flags ([Resources](../user_guide/orchestration.md#resources))."

- [ ] **Step 3: Verify**

Run: `grep -rn "resources-docker-kubernetes\|Kubernetes-style\|parse_quantity" aaiclick docs test_e2e`
Expected: no output.

- [ ] **Step 4: Remove spec and plan, commit** — `Docs: runner-native resources`

- [ ] **Step 5: Push and run `check-pr`**
