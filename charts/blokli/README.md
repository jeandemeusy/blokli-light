# blokli-light Helm chart

This chart runs the raw-log daemon only. It has no Service or Ingress because blokli-light exposes no HTTP API. Supply PostgreSQL and RPC URLs directly or through existing Secrets; the pod uses PostgreSQL advisory locking to prevent concurrent writers.

CI publishes `linux/amd64` images to `ghcr.io/jeandemeusy/blokli-light`. Deploy an immutable commit or release tag:

```bash
helm upgrade --install blokli-light charts/blokli \
  --set image.repository=ghcr.io/jeandemeusy/blokli-light \
  --set image.tag=sha-0123456 \
  --set-string database.url='postgresql://blokli_light:password@postgres.example:5432/blokli_light' \
  --set-string rpc.url='https://your-gnosis-rpc.example' \
  --set-string reader.password='replace-with-a-long-random-password'
```

If the GHCR package remains private, create a registry secret in the target namespace and pass it to the chart:

```bash
kubectl create secret docker-registry ghcr \
  --docker-server=ghcr.io \
  --docker-username=YOUR_GITHUB_USER \
  --docker-password="$GHCR_TOKEN"

helm upgrade --install blokli-light charts/blokli \
  --set 'imagePullSecrets[0].name=ghcr'
```

`GHCR_TOKEN` needs `read:packages`.
