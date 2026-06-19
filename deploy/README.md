# collaboration-service deploy manifests (WS-E build-ahead)

> **ACCESS FLAG — these manifests live here temporarily.** They belong in the
> `alkem-io/infrastructure-operations` repo (branch `feat/003-unify-collab-yjs`,
> per `specs/003-unify-collab-yjs/tasks/infrastructure-operations.md` T001–T003).
> At build time `infrastructure-operations` was **not accessible** to the author
> (`git ls-remote https://github.com/alkem-io/infrastructure-operations` →
> *repository not found*; the org-admin / infra-ops access transition is pending —
> see the workspace memory note). So they were written here, in the migration PR,
> as the **artifact to be moved over** once infra-ops access lands. **Nothing here
> is applied to any cluster by this PR.**

## What this is

Kustomize manifests for deploying the unified `collaboration-service` to the two
GitOps environments, covering infra-ops **T001** (deploy) and **T002**
(observability), with **T003** (secrets) as a committed template.

```
deploy/k8s/
├── base/                      # environment-agnostic
│   ├── deployment.yaml        # WS service: /healthz probes, graceful drain, envFrom
│   ├── service.yaml           # ClusterIP :4006
│   ├── ingress.yaml           # WS-CAPABLE ingress (upgrade + 1h idle, no buffering)
│   ├── configmap.yaml         # non-secret env: FANOUT=redis, METADATA=rabbitmq,
│   │                          #   BLOB=file-service, AUTH=authzeval, limits, AUTH_SERVICE_URL
│   ├── secret.example.yaml    # T003 secret SHAPE (Redis/RMQ creds) — no real values
│   ├── redis.yaml             # cross-pod fan-out bus (ephemeral pub/sub relay)
│   ├── hpa.yaml               # CPU/mem HPA (min 2, slow scale-down for long WS)
│   ├── observability.yaml     # T002: ServiceMonitor + PrometheusRule (real metric names)
│   └── kustomization.yaml
└── overlays/
    ├── acceptance/            # ACC (deploy PR → develop): 2 replicas, acc domain
    └── prod/                  # PROD (draft deploy PR → main): 3–12 replicas, prod domain
```

Validate locally:

```bash
kustomize build deploy/k8s/overlays/acceptance
kustomize build deploy/k8s/overlays/prod
```

(Both build clean with kustomize v5.)

## When infra-ops access lands — how to move it over

1. Clone `infrastructure-operations`, branch `feat/003-unify-collab-yjs`.
2. Copy `deploy/k8s/` into the repo's collaboration-service path, conforming to
   the repo's existing Kustomize/Helm layout (it may prefer per-service dirs or a
   Helm chart — adapt the base/overlay split accordingly).
3. Replace the placeholder env-reference UUIDs (`*-FILE-SERVICE-BUCKET-UUID`) and
   wire the real secrets via that environment's mechanism (sealed secrets today,
   Vault/Pulumi per the transition — T003).
4. Set the image tag via the release-deploy flow (it bumps `newTag`).
5. The PROD deploy PR stays **draft** and is human-merged (release-deploy / the
   production-deploy human gate). **Do not auto-merge it.**

## What is deliberately NOT here

- The legacy-service **decommission** manifests (infra-ops T007, Phase 3) — that
  is the *last* step, after the confidence period, and is human-gated. Out of
  scope for this build-ahead.
- The big-bang **cutover** wiring (traffic flip) — see
  `docs/migration-cutover-runbook.md`; human-gated.
