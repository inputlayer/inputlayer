# TLS Deployment Guide

InputLayer does not terminate TLS natively. Use a reverse proxy for TLS termination in production.

## Nginx

```nginx
server {
    listen 443 ssl;
    server_name inputlayer.example.com;

    ssl_certificate     /etc/ssl/certs/inputlayer.crt;
    ssl_certificate_key /etc/ssl/private/inputlayer.key;
    ssl_protocols       TLSv1.2 TLSv1.3;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;

        # WebSocket support
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;

        # Forward real client IP for per-IP rate limiting
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Increase timeouts for long-running queries
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }
}
```

## Caddy

```caddyfile
inputlayer.example.com {
    reverse_proxy 127.0.0.1:8080 {
        # WebSocket auto-detected by Caddy
        header_up X-Real-IP {remote_host}
    }
}
```

Caddy automatically provisions and renews Let's Encrypt certificates.

## Kubernetes Ingress

Use the ingress-nginx Ingress shipped in
`deploy/kubernetes/components/ingress/ingress.yaml`. It terminates TLS from
the `inputlayer-tls` secret (for example, issued by cert-manager) and keeps idle
WebSocket subscriptions open for an hour. Enable it from a tenant overlay, as
in `deploy/kubernetes/overlays/tenant-example`. Set `http.trusted_proxies` to
the ingress controller's pod CIDR. See the Kubernetes section of the
Deployment guide (`docs/content/docs/guides/deployment.mdx`).

## Docker Compose with Traefik

```yaml
services:
  traefik:
    image: traefik:v3.0
    command:
      - "--entrypoints.websecure.address=:443"
      - "--certificatesresolvers.letsencrypt.acme.email=admin@example.com"
      - "--certificatesresolvers.letsencrypt.acme.storage=/acme/acme.json"
      - "--certificatesresolvers.letsencrypt.acme.httpchallenge.entrypoint=web"
    ports:
      - "443:443"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - acme:/acme

  inputlayer:
    image: inputlayer:latest
    labels:
      - "traefik.http.routers.inputlayer.rule=Host(`inputlayer.example.com`)"
      - "traefik.http.routers.inputlayer.tls.certresolver=letsencrypt"
      - "traefik.http.services.inputlayer.loadbalancer.server.port=8080"

volumes:
  acme:
```

## Security Recommendations

1. **Bind to localhost**: Set `host = "127.0.0.1"` in config to prevent direct access
2. **Trust proxy headers**: Set `trusted_proxies = ["127.0.0.1"]` (IPs or CIDRs) under `[http]` so InputLayer reads `X-Forwarded-For` (or `X-Real-IP` when it is absent) from your proxy. Otherwise the TCP peer is the client IP
3. **Disable CORS in production**: Only enable `cors_allow_all` for development
4. **Use strong TLS**: Minimum TLS 1.2, prefer TLS 1.3
5. **Rate limit at proxy level too**: Defense-in-depth with both proxy and InputLayer rate limits
