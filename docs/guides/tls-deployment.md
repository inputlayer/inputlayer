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

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: inputlayer
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "300"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "300"
    cert-manager.io/cluster-issuer: letsencrypt-prod
spec:
  tls:
    - hosts:
        - inputlayer.example.com
      secretName: inputlayer-tls
  rules:
    - host: inputlayer.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: inputlayer
                port:
                  number: 8080
```

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
2. **Trust proxy headers**: InputLayer reads `X-Forwarded-For` and `X-Real-IP` for rate limiting
3. **Disable CORS in production**: Only enable `cors_allow_all` for development
4. **Use strong TLS**: Minimum TLS 1.2, prefer TLS 1.3
5. **Rate limit at proxy level too**: Defense-in-depth with both proxy and InputLayer rate limits
