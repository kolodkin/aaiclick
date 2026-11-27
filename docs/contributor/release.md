Release
---

How aaiclick ships: the release pipeline and Java publishing.

# Release pipeline

**Implementation**: `.github/workflows/publish.yaml`

```
build ──► build-rc-images ──► merge-rc-images ──► gates (parallel) ──► publish (PyPI) ──► promote-images ──► publish-chart
```

## build-rc-images — per-platform matrix

The three images are FROM-chained (`aaiclick` → `aaiclick-docker` →
`aaiclick-kubectl`), so they cannot build in parallel per image. They
parallelize per platform instead, the larger win — a QEMU-emulated arm64 leg
would otherwise dominate build time:

- Matrix over `platform: [amd64, arm64]`; arm64 runs on the native
  `ubuntu-24.04-arm` runner. Each leg serves the wheel artifact from a local
  pypiserver, builds the three images natively in FROM order (the base
  through `.github/actions/build-base-image`), and pushes per-arch digests to
  GHCR.
- A merge job (matrix over the three images) assembles each multi-arch
  `vX.Y.Z-rc` manifest with `docker buildx imagetools create`.

The wheel baked into the rc images is the same file later uploaded to PyPI.

## Gates — all parallel, all required

Every gate depends only on `build` + `merge-rc-images` (or `build` alone for
the local-only gate), so they run concurrently.

| Gate                    | Shape                                                                                                                                                                                                                                                 |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| test-package-local      | chdb + SQLite, no infra                                                                                                                                                                                                                               |
| test-package            | Data/orch matrix; infra from `deploy/compose` (`env.init.sh`, full stack up, waiting for `migrate` to exit 0) instead of `services:` blocks                                                                                                           |
| test-compose-e2e        | `deploy/compose` + `env.init.sh`, `AAICLICK_IMAGE_TAG` set to `-rc`, `docker compose up`, wait for server health, then `docker compose exec` into the server to register/run a job via the CLI and poll `job list --json`                            |
| test-helm-e2e           | k3s cluster with the `-rc` images side-loaded (`k3s ctr images import`), `helm install` of `deploy/helm/aaiclick`, a port-forwarded server-health smoke, a `kubectl exec` job round trip (`register-job` / `run-job` / `job list --json`), then `helm upgrade` must leave the credentials Secret unchanged |
| test-package-docker-e2e | `_docker-e2e-reusable.yaml`; `base_image` is the `-rc` base, so git builds without a Dockerfile layer on the promoted digest                                                                                                                          |

## promote-images

After PyPI publish succeeds, `docker buildx imagetools create -t vX.Y.Z -t
latest <image>:vX.Y.Z-rc` per image (matrix over the three names) — no
rebuild, no wait-for-PyPI polling. A final step deletes the `-rc` tags on
success; failed releases keep them for debugging.

## publish-chart

After `promote-images`, so the chart never names an image tag that does not
exist yet: `helm package deploy/helm/aaiclick --version X.Y.Z --app-version
vX.Y.Z`, then `helm push` to `oci://ghcr.io/kolodkin/charts`.

# Java publishing

**Implementation**: `.github/workflows/publish.yaml`

`aaiclick-task-api` publishes to Maven Central via the Central Publisher
Portal on the **same tag** as the Python package — lockstep versioning; the
compatibility contract is a release's file data plane and task semantics.

- Namespace `io.github.kolodkin` auto-verifies against the GitHub account.
- The parent POM uses the CI-friendly `${revision}` version
  (flatten-maven-plugin); the publish job passes `-Drevision=X.Y.Z` from the
  tag.
- The `release` profile adds GPG signing plus sources/javadoc jars and the
  `central-publishing-maven-plugin`.
- Secrets: `MAVEN_CENTRAL_USERNAME` / `MAVEN_CENTRAL_PASSWORD` (portal
  token), `MAVEN_GPG_PRIVATE_KEY` / `MAVEN_GPG_PASSPHRASE`.
- De-risk with a one-time `0.0.x` dry-run publish (manual
  `workflow_dispatch`) before the first real lockstep release.
- The Java leg runs after the PyPI upload, so the release is gated on
  `mvn verify` at the release version first: a broken jar blocks the
  irreversible upload instead of orphaning a tagged Python release.
