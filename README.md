# Spark ETL - API Specifications

## Vue d'ensemble du projet

Spark ETL est un système d'Extract-Transform-Load (ETL) avec une interface graphique qui utilise Apache Spark comme moteur d'exécution sous-jacent. Ce projet vise à simplifier la création et la gestion de pipelines de données pour les utilisateurs, tout en offrant la puissance et la scalabilité de Spark.

## Objectifs

- **Facilité d'utilisation**: Interface graphique intuitive pour créer et gérer des pipelines ETL
- **Puissance**: Utilisation d'Apache Spark pour traiter de grands volumes de données
- **Flexibilité**: Support de multiples sources et destinations de données
- **Observabilité**: Monitoring en temps réel et historique détaillé des exécutions
- **Sécurité**: Gestion sécurisée des credentials et authentification des utilisateurs

## Architecture

```
┌─────────────────────────────────────────┐
│         Interface Graphique             │
│         (Frontend Web App)              │
└────────────────┬────────────────────────┘
                 │ REST API / WebSocket
                 │
┌────────────────▼────────────────────────┐
│         API Spring Boot                 │
│  ┌──────────────────────────────────┐   │
│  │  Controllers (REST Endpoints)    │   │
│  └────────────┬─────────────────────┘   │
│  ┌────────────▼─────────────────────┐   │
│  │  Services (Business Logic)       │   │
│  └────────────┬─────────────────────┘   │
│  ┌────────────▼─────────────────────┐   │
│  │  Repositories (Data Access)      │   │
│  └────────────┬─────────────────────┘   │
│               │                          │
│  ┌────────────▼─────────────────────┐   │
│  │  Spark Execution Engine          │   │
│  └──────────────────────────────────┘   │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│         Base de données                 │
│  (PostgreSQL / MySQL)                   │
└─────────────────────────────────────────┘
```

## Documentation

### Spécifications

1. **[API_SPECIFICATIONS.md](./API_SPECIFICATIONS.md)** - Spécifications complètes de l'API REST
   - Modèles de données
   - Endpoints REST détaillés
   - Gestion des erreurs
   - Sécurité et authentification
   - WebSocket pour les mises à jour en temps réel

2. **[DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md)** - Schéma de base de données
   - Structure des tables
   - Relations et contraintes
   - Index et optimisations
   - Vues et triggers
   - Scripts de migration

## Fonctionnalités principales

### 1. Gestion des projets
- Créer, lister, modifier et supprimer des projets
- Organiser les pipelines par projet
- Gestion des membres et permissions (future)

### 2. Gestion des credentials
- Configurer les credentials pour les sources et destinations
- Support de multiples types: Database, S3, Azure Blob, GCS, SFTP, HTTP, Kafka
- Chiffrement des données sensibles

### 3. Gestion des pipelines
- Créer et configurer des pipelines ETL
- Définir les sources, transformations et destinations
- Sauvegarder et versionner les spécifications
- Activer/désactiver des pipelines

### 4. Exécution et monitoring
- Exécuter des pipelines de manière asynchrone
- Suivre l'état d'exécution en temps réel
- Voir la progression et les statistiques
- Annuler les exécutions en cours

### 5. Historique et logs
- Consulter l'historique complet des exécutions
- Accéder aux logs détaillés
- Analyser les métriques de performance
- Télécharger les logs pour analyse

## Technologies

### Backend
- **Java 17+** - Langage de programmation
- **Spring Boot 3.x** - Framework web
- **Spring Data JPA** - Accès aux données
- **Spring Security** - Authentification et autorisation
- **Spring WebSocket** - Communication temps réel
- **Apache Spark 3.x** - Moteur d'exécution ETL

### Base de données
- **PostgreSQL** (recommandé) - Base de données principale
- Support de **MySQL** comme alternative

### Monitoring et observabilité
- **Spring Boot Actuator** - Health checks et métriques
- **Prometheus** - Collecte de métriques
- **Grafana** - Visualisation

### Autres
- **Redis** - Cache et queue de jobs
- **JWT** - Authentification stateless

## Prochaines étapes

### Phase 1: Spécifications (Actuelle) ✅
- [x] Définir les endpoints API
- [x] Définir le schéma de base de données
- [x] Documenter les modèles de données

### Phase 2: Mise en place du projet
- [ ] Initialiser le projet Spring Boot
- [ ] Configurer les dépendances Maven/Gradle
- [ ] Mettre en place la structure du projet
- [ ] Configurer la base de données

### Phase 3: Implémentation du backend
- [ ] Créer les entités JPA
- [ ] Implémenter les repositories
- [ ] Implémenter les services métier
- [ ] Implémenter les controllers REST
- [ ] Ajouter la validation des données

### Phase 4: Sécurité
- [ ] Implémenter l'authentification JWT
- [ ] Configurer Spring Security
- [ ] Implémenter le chiffrement des credentials
- [ ] Ajouter les contrôles d'autorisation

### Phase 5: Moteur d'exécution Spark
- [ ] Intégrer Apache Spark
- [ ] Implémenter l'exécution des pipelines
- [ ] Gérer la queue de jobs
- [ ] Implémenter le monitoring des jobs
- [ ] Gérer les logs d'exécution

### Phase 6: WebSocket et temps réel
- [ ] Configurer WebSocket
- [ ] Implémenter les notifications temps réel
- [ ] Gérer les mises à jour de progression

### Phase 7: Tests
- [ ] Tests unitaires
- [ ] Tests d'intégration
- [ ] Tests de performance
- [ ] Tests de sécurité

### Phase 8: Documentation et déploiement
- [ ] Générer la documentation Swagger/OpenAPI
- [ ] Créer le guide de déploiement
- [ ] Dockeriser l'application
- [ ] Préparer les scripts de déploiement

## Contribution

### Structure du code (future)
```
src/
├── main/
│   ├── java/
│   │   └── com/example/sparketl/
│   │       ├── config/          # Configuration Spring
│   │       ├── controller/      # REST Controllers
│   │       ├── dto/             # Data Transfer Objects
│   │       ├── entity/          # JPA Entities
│   │       ├── exception/       # Custom Exceptions
│   │       ├── repository/      # JPA Repositories
│   │       ├── security/        # Security Config
│   │       ├── service/         # Business Logic
│   │       └── spark/           # Spark Integration
│   └── resources/
│       ├── application.yml      # Configuration
│       └── db/migration/        # Flyway Migrations
└── test/
    └── java/
        └── com/example/sparketl/
```

## Licence

À définir

## Contact

À définir

---

**Note**: Ce document fait partie des spécifications initiales du projet. Les détails d'implémentation seront ajoutés au fur et à mesure de l'avancement du projet.
