## Description

Ce projet implémente un load balancer en Rust avec les fonctionnalités suivantes :

- **Vérifications de santé** : Périodiquement, le load balancer envoie des requêtes de santé aux serveurs en amont pour vérifier leur état. Si un serveur répond correctement, il est marqué comme sain ; sinon, il est marqué comme hors service. Cela permet de s'assurer que le trafic est dirigé uniquement vers des serveurs fonctionnels.
- **Limitation de taux** : Pour éviter les abus et protéger les serveurs en amont, chaque adresse IP est limitée à un nombre maximal de requêtes par fenêtre de temps définie. Si une adresse IP dépasse cette limite, elle reçoit une réponse 429 (Too Many Requests).
- **Gestion multithread** : Le load balancer utilise un pool de threads pour gérer les connexions entrantes de manière concurrente, améliorant ainsi les performances et la capacité de traitement. Cela permet de gérer efficacement de nombreuses connexions simultanées.

## Prérequis

- Rust et Cargo installés
- La crate `threadpool` pour la gestion des threads
- La crate `reqwest` pour les requêtes HTTP
- Installation d'OpenSSL et des packages de développement

### Installation d'OpenSSL

1. Installer OpenSSL et ses dépendances de développement

```bash
sudo apt-get update
sudo apt-get install -y pkg-config libssl-dev
```

2. Configurer les variables d'environnement

Parfois, il est nécessaire de définir la variable d'environnement `PKG_CONFIG_PATH` pour pointer vers les fichiers de configuration d'OpenSSL. Cependant, après l'installation de `libssl-dev`, cela n'est généralement pas nécessaire.

## Utilisation

### 1. Démarrer les serveurs en amont :

```bash
python3 -m http.server 8081
python3 -m http.server 8082
```

### 2. Exécuter le load balancer :

```bash
cargo run
```

### 3. Tester avec curl :

```bash
curl http://127.0.0.1:8080
```

Le load balancer redirigera la requête vers l'un des serveurs en amont disponibles et retournera la réponse au client.
