# Déploiement sur TrueNAS SCALE

## Option 1 — Custom App (YAML Compose) dès maintenant
1. Construis et pousse l'image (ou build localement sur un hôte Docker) :
   ```bash
   docker build -f deploy/Dockerfile.daemon -t gens-relay-daemon:2.0.0 .
   ```
2. Dans **TrueNAS SCALE → Apps → Discover Apps → Custom App (Install via YAML)**, utilise `deploy/docker-compose.yml` en remplaçant `/mnt/tank/sync` par tes vrais datasets ZFS.
3. Ouvre `http://<IP-TRUENAS>:8385`, va dans **Paramètres** et définis un utilisateur + mot de passe pour protéger la Web UI.

## Option 2 — Publication en App officielle / communautaire TrueNAS
1. Publier l'image multi-arch (`linux/amd64`, `linux/arm64`) sur GitHub Container Registry (`ghcr.io/<user>/gens-relay-daemon`).
2. Compléter le template Docker Compose TrueNAS SCALE (`templates/library/base_v2_x_x` + `app.yaml`) dans un fork de `truenas/apps` et ouvrir une Pull Request dans la catégorie `community` (ou héberger un catalogue personnalisé).
