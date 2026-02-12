# Spark ETL API Specifications

## Vue d'ensemble

Cette API Spring Boot fournit les endpoints REST pour gérer un système ETL (Extract, Transform, Load) basé sur Apache Spark avec interface graphique.

## Architecture

- **Backend**: Java Spring Boot
- **Moteur d'exécution**: Apache Spark
- **Style API**: REST
- **Format de données**: JSON

## Modèles de données

### Project
Représente un projet de pipeline ETL qui regroupe plusieurs pipelines et leurs configurations.

```json
{
  "id": "string (UUID)",
  "name": "string",
  "description": "string",
  "createdAt": "datetime (ISO 8601)",
  "updatedAt": "datetime (ISO 8601)"
}
```

### Credential
Représente les informations d'authentification pour les sources et destinations de données.

```json
{
  "id": "string (UUID)",
  "projectId": "string (UUID)",
  "name": "string",
  "type": "string (enum: DATABASE, S3, AZURE_BLOB, GCS, SFTP, HTTP, KAFKA)",
  "properties": {
    "key": "value"
  },
  "encrypted": "boolean",
  "createdAt": "datetime (ISO 8601)",
  "updatedAt": "datetime (ISO 8601)"
}
```

### Pipeline
Représente une définition de pipeline ETL avec sa configuration.

```json
{
  "id": "string (UUID)",
  "projectId": "string (UUID)",
  "name": "string",
  "description": "string",
  "spec": {
    "source": {
      "type": "string",
      "credentialId": "string (UUID)",
      "config": {}
    },
    "transformations": [
      {
        "type": "string",
        "config": {}
      }
    ],
    "destination": {
      "type": "string",
      "credentialId": "string (UUID)",
      "config": {}
    }
  },
  "enabled": "boolean",
  "createdAt": "datetime (ISO 8601)",
  "updatedAt": "datetime (ISO 8601)"
}
```

### Job
Représente une exécution de pipeline.

```json
{
  "id": "string (UUID)",
  "pipelineId": "string (UUID)",
  "status": "string (enum: PENDING, RUNNING, COMPLETED, FAILED, CANCELLED)",
  "startedAt": "datetime (ISO 8601)",
  "completedAt": "datetime (ISO 8601)",
  "progress": "number (0-100)",
  "statistics": {
    "recordsProcessed": "number",
    "recordsFailed": "number",
    "bytesProcessed": "number"
  },
  "errorMessage": "string",
  "createdAt": "datetime (ISO 8601)"
}
```

### JobLog
Représente les logs d'exécution d'un job.

```json
{
  "id": "string (UUID)",
  "jobId": "string (UUID)",
  "timestamp": "datetime (ISO 8601)",
  "level": "string (enum: DEBUG, INFO, WARN, ERROR)",
  "message": "string",
  "metadata": {}
}
```

## Endpoints API

### 1. Gestion des Projets

#### 1.1 Lister tous les projets
```
GET /api/v1/projects
```

**Query Parameters:**
- `page` (optional): numéro de page (default: 0)
- `size` (optional): taille de la page (default: 20)
- `sort` (optional): champ de tri (default: "createdAt")
- `order` (optional): ordre (asc/desc, default: "desc")

**Response:** `200 OK`
```json
{
  "content": [
    {
      "id": "uuid",
      "name": "Mon Projet ETL",
      "description": "Description du projet",
      "createdAt": "2026-02-12T10:00:00Z",
      "updatedAt": "2026-02-12T10:00:00Z"
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 1,
  "totalPages": 1
}
```

#### 1.2 Obtenir un projet par ID
```
GET /api/v1/projects/{projectId}
```

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "name": "Mon Projet ETL",
  "description": "Description du projet",
  "createdAt": "2026-02-12T10:00:00Z",
  "updatedAt": "2026-02-12T10:00:00Z"
}
```

**Error:** `404 Not Found` si le projet n'existe pas

#### 1.3 Créer un nouveau projet
```
POST /api/v1/projects
```

**Request Body:**
```json
{
  "name": "Mon Projet ETL",
  "description": "Description du projet"
}
```

**Response:** `201 Created`
```json
{
  "id": "uuid",
  "name": "Mon Projet ETL",
  "description": "Description du projet",
  "createdAt": "2026-02-12T10:00:00Z",
  "updatedAt": "2026-02-12T10:00:00Z"
}
```

**Error:** `400 Bad Request` si les données sont invalides

#### 1.4 Mettre à jour un projet
```
PUT /api/v1/projects/{projectId}
```

**Request Body:**
```json
{
  "name": "Nouveau nom",
  "description": "Nouvelle description"
}
```

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "name": "Nouveau nom",
  "description": "Nouvelle description",
  "createdAt": "2026-02-12T10:00:00Z",
  "updatedAt": "2026-02-12T13:00:00Z"
}
```

#### 1.5 Supprimer un projet
```
DELETE /api/v1/projects/{projectId}
```

**Response:** `204 No Content`

**Error:** 
- `404 Not Found` si le projet n'existe pas
- `409 Conflict` si le projet contient des pipelines actifs

---

### 2. Gestion des Credentials

#### 2.1 Lister les credentials d'un projet
```
GET /api/v1/projects/{projectId}/credentials
```

**Response:** `200 OK`
```json
[
  {
    "id": "uuid",
    "projectId": "uuid",
    "name": "Database Production",
    "type": "DATABASE",
    "properties": {
      "host": "db.example.com",
      "port": "5432",
      "database": "mydb",
      "username": "user"
    },
    "encrypted": true,
    "createdAt": "2026-02-12T10:00:00Z",
    "updatedAt": "2026-02-12T10:00:00Z"
  }
]
```

#### 2.2 Obtenir un credential par ID
```
GET /api/v1/projects/{projectId}/credentials/{credentialId}
```

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "projectId": "uuid",
  "name": "Database Production",
  "type": "DATABASE",
  "properties": {
    "host": "db.example.com",
    "port": "5432",
    "database": "mydb",
    "username": "user"
  },
  "encrypted": true,
  "createdAt": "2026-02-12T10:00:00Z",
  "updatedAt": "2026-02-12T10:00:00Z"
}
```

#### 2.3 Créer un nouveau credential
```
POST /api/v1/projects/{projectId}/credentials
```

**Request Body:**
```json
{
  "name": "Database Production",
  "type": "DATABASE",
  "properties": {
    "host": "db.example.com",
    "port": "5432",
    "database": "mydb",
    "username": "user",
    "password": "secret"
  }
}
```

**Response:** `201 Created`
```json
{
  "id": "uuid",
  "projectId": "uuid",
  "name": "Database Production",
  "type": "DATABASE",
  "properties": {
    "host": "db.example.com",
    "port": "5432",
    "database": "mydb",
    "username": "user"
  },
  "encrypted": true,
  "createdAt": "2026-02-12T10:00:00Z",
  "updatedAt": "2026-02-12T10:00:00Z"
}
```

**Note:** Les propriétés sensibles (passwords, tokens) sont automatiquement chiffrées et ne sont jamais retournées dans les réponses.

#### 2.4 Mettre à jour un credential
```
PUT /api/v1/projects/{projectId}/credentials/{credentialId}
```

**Request Body:**
```json
{
  "name": "Database Production Updated",
  "type": "DATABASE",
  "properties": {
    "host": "db.example.com",
    "port": "5432",
    "database": "mydb",
    "username": "newuser",
    "password": "newsecret"
  }
}
```

**Response:** `200 OK`

#### 2.5 Supprimer un credential
```
DELETE /api/v1/projects/{projectId}/credentials/{credentialId}
```

**Response:** `204 No Content`

**Error:** `409 Conflict` si le credential est utilisé par un pipeline

---

### 3. Gestion des Pipelines

#### 3.1 Lister les pipelines d'un projet
```
GET /api/v1/projects/{projectId}/pipelines
```

**Query Parameters:**
- `page` (optional): numéro de page (default: 0)
- `size` (optional): taille de la page (default: 20)
- `enabled` (optional): filtrer par statut (true/false)

**Response:** `200 OK`
```json
{
  "content": [
    {
      "id": "uuid",
      "projectId": "uuid",
      "name": "ETL Customers",
      "description": "Pipeline pour traiter les données clients",
      "spec": {
        "source": {
          "type": "database",
          "credentialId": "uuid",
          "config": {
            "query": "SELECT * FROM customers"
          }
        },
        "transformations": [
          {
            "type": "filter",
            "config": {
              "condition": "age > 18"
            }
          }
        ],
        "destination": {
          "type": "s3",
          "credentialId": "uuid",
          "config": {
            "bucket": "my-bucket",
            "path": "customers/"
          }
        }
      },
      "enabled": true,
      "createdAt": "2026-02-12T10:00:00Z",
      "updatedAt": "2026-02-12T10:00:00Z"
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 1,
  "totalPages": 1
}
```

#### 3.2 Obtenir un pipeline par ID
```
GET /api/v1/projects/{projectId}/pipelines/{pipelineId}
```

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "projectId": "uuid",
  "name": "ETL Customers",
  "description": "Pipeline pour traiter les données clients",
  "spec": {
    "source": {
      "type": "database",
      "credentialId": "uuid",
      "config": {
        "query": "SELECT * FROM customers"
      }
    },
    "transformations": [
      {
        "type": "filter",
        "config": {
          "condition": "age > 18"
        }
      }
    ],
    "destination": {
      "type": "s3",
      "credentialId": "uuid",
      "config": {
        "bucket": "my-bucket",
        "path": "customers/"
      }
    }
  },
  "enabled": true,
  "createdAt": "2026-02-12T10:00:00Z",
  "updatedAt": "2026-02-12T10:00:00Z"
}
```

#### 3.3 Créer un nouveau pipeline
```
POST /api/v1/projects/{projectId}/pipelines
```

**Request Body:**
```json
{
  "name": "ETL Customers",
  "description": "Pipeline pour traiter les données clients",
  "spec": {
    "source": {
      "type": "database",
      "credentialId": "uuid",
      "config": {
        "query": "SELECT * FROM customers"
      }
    },
    "transformations": [
      {
        "type": "filter",
        "config": {
          "condition": "age > 18"
        }
      }
    ],
    "destination": {
      "type": "s3",
      "credentialId": "uuid",
      "config": {
        "bucket": "my-bucket",
        "path": "customers/"
      }
    }
  },
  "enabled": true
}
```

**Response:** `201 Created`

#### 3.4 Mettre à jour un pipeline (sauvegarder spec)
```
PUT /api/v1/projects/{projectId}/pipelines/{pipelineId}
```

**Request Body:**
```json
{
  "name": "ETL Customers Updated",
  "description": "Pipeline mis à jour",
  "spec": {
    "source": {
      "type": "database",
      "credentialId": "uuid",
      "config": {
        "query": "SELECT * FROM customers WHERE active = true"
      }
    },
    "transformations": [
      {
        "type": "filter",
        "config": {
          "condition": "age > 18"
        }
      },
      {
        "type": "map",
        "config": {
          "fields": ["id", "name", "email"]
        }
      }
    ],
    "destination": {
      "type": "s3",
      "credentialId": "uuid",
      "config": {
        "bucket": "my-bucket",
        "path": "customers/"
      }
    }
  },
  "enabled": true
}
```

**Response:** `200 OK`

#### 3.5 Supprimer un pipeline
```
DELETE /api/v1/projects/{projectId}/pipelines/{pipelineId}
```

**Response:** `204 No Content`

**Error:** `409 Conflict` si des jobs sont en cours d'exécution pour ce pipeline

---

### 4. Exécution des Pipelines

#### 4.1 Exécuter un pipeline (asynchrone)
```
POST /api/v1/projects/{projectId}/pipelines/{pipelineId}/execute
```

**Request Body (optional):**
```json
{
  "parameters": {
    "startDate": "2026-01-01",
    "endDate": "2026-02-01"
  }
}
```

**Response:** `202 Accepted`
```json
{
  "id": "uuid",
  "pipelineId": "uuid",
  "status": "PENDING",
  "createdAt": "2026-02-12T13:00:00Z"
}
```

**Error:**
- `404 Not Found` si le pipeline n'existe pas
- `400 Bad Request` si le pipeline n'est pas activé
- `409 Conflict` si une exécution est déjà en cours

---

### 5. Gestion des Jobs

#### 5.1 Lister les jobs en attente ou en cours
```
GET /api/v1/jobs?status=PENDING,RUNNING
```

**Query Parameters:**
- `status` (optional): filtrer par statut (comma-separated)
- `projectId` (optional): filtrer par projet
- `pipelineId` (optional): filtrer par pipeline
- `page` (optional): numéro de page (default: 0)
- `size` (optional): taille de la page (default: 20)

**Response:** `200 OK`
```json
{
  "content": [
    {
      "id": "uuid",
      "pipelineId": "uuid",
      "pipelineName": "ETL Customers",
      "projectId": "uuid",
      "projectName": "Mon Projet ETL",
      "status": "RUNNING",
      "startedAt": "2026-02-12T13:00:00Z",
      "progress": 45.5,
      "statistics": {
        "recordsProcessed": 1000000,
        "recordsFailed": 10,
        "bytesProcessed": 104857600
      },
      "createdAt": "2026-02-12T12:55:00Z"
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 1,
  "totalPages": 1
}
```

#### 5.2 Obtenir le statut d'un job
```
GET /api/v1/jobs/{jobId}
```

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "pipelineId": "uuid",
  "pipelineName": "ETL Customers",
  "projectId": "uuid",
  "projectName": "Mon Projet ETL",
  "status": "RUNNING",
  "startedAt": "2026-02-12T13:00:00Z",
  "completedAt": null,
  "progress": 45.5,
  "statistics": {
    "recordsProcessed": 1000000,
    "recordsFailed": 10,
    "bytesProcessed": 104857600
  },
  "errorMessage": null,
  "createdAt": "2026-02-12T12:55:00Z"
}
```

#### 5.3 Annuler l'exécution d'un job
```
POST /api/v1/jobs/{jobId}/cancel
```

**Response:** `200 OK`
```json
{
  "id": "uuid",
  "pipelineId": "uuid",
  "status": "CANCELLED",
  "startedAt": "2026-02-12T13:00:00Z",
  "completedAt": "2026-02-12T13:15:00Z",
  "progress": 45.5,
  "statistics": {
    "recordsProcessed": 1000000,
    "recordsFailed": 10,
    "bytesProcessed": 104857600
  },
  "createdAt": "2026-02-12T12:55:00Z"
}
```

**Error:**
- `404 Not Found` si le job n'existe pas
- `400 Bad Request` si le job est déjà terminé ou annulé

---

### 6. Historique et Logs des Jobs

#### 6.1 Lister l'historique des jobs
```
GET /api/v1/jobs/history
```

**Query Parameters:**
- `projectId` (optional): filtrer par projet
- `pipelineId` (optional): filtrer par pipeline
- `status` (optional): filtrer par statut
- `startDate` (optional): date de début (ISO 8601)
- `endDate` (optional): date de fin (ISO 8601)
- `page` (optional): numéro de page (default: 0)
- `size` (optional): taille de la page (default: 20)
- `sort` (optional): champ de tri (default: "createdAt")
- `order` (optional): ordre (asc/desc, default: "desc")

**Response:** `200 OK`
```json
{
  "content": [
    {
      "id": "uuid",
      "pipelineId": "uuid",
      "pipelineName": "ETL Customers",
      "projectId": "uuid",
      "projectName": "Mon Projet ETL",
      "status": "COMPLETED",
      "startedAt": "2026-02-12T10:00:00Z",
      "completedAt": "2026-02-12T10:30:00Z",
      "progress": 100,
      "statistics": {
        "recordsProcessed": 5000000,
        "recordsFailed": 25,
        "bytesProcessed": 524288000
      },
      "createdAt": "2026-02-12T09:55:00Z"
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 150,
  "totalPages": 8
}
```

#### 6.2 Obtenir les logs d'un job
```
GET /api/v1/jobs/{jobId}/logs
```

**Query Parameters:**
- `level` (optional): filtrer par niveau de log (DEBUG, INFO, WARN, ERROR)
- `page` (optional): numéro de page (default: 0)
- `size` (optional): taille de la page (default: 100)
- `since` (optional): timestamp ISO 8601 pour obtenir les logs depuis une date

**Response:** `200 OK`
```json
{
  "content": [
    {
      "id": "uuid",
      "jobId": "uuid",
      "timestamp": "2026-02-12T10:00:00Z",
      "level": "INFO",
      "message": "Pipeline execution started",
      "metadata": {
        "stage": "initialization"
      }
    },
    {
      "id": "uuid",
      "jobId": "uuid",
      "timestamp": "2026-02-12T10:05:00Z",
      "level": "INFO",
      "message": "Processing records: 1000000",
      "metadata": {
        "stage": "processing",
        "recordsProcessed": 1000000
      }
    },
    {
      "id": "uuid",
      "jobId": "uuid",
      "timestamp": "2026-02-12T10:15:00Z",
      "level": "WARN",
      "message": "Skipping invalid record",
      "metadata": {
        "recordId": "12345",
        "reason": "Missing required field"
      }
    }
  ],
  "page": 0,
  "size": 100,
  "totalElements": 234,
  "totalPages": 3
}
```

#### 6.3 Télécharger les logs complets d'un job
```
GET /api/v1/jobs/{jobId}/logs/download
```

**Response:** `200 OK`
- Content-Type: `text/plain` ou `application/gzip`
- Fichier de logs complet

---

## Gestion des erreurs

Toutes les erreurs suivent le format standardisé suivant:

```json
{
  "timestamp": "2026-02-12T13:00:00Z",
  "status": 400,
  "error": "Bad Request",
  "message": "Validation failed for field 'name': must not be blank",
  "path": "/api/v1/projects",
  "details": [
    {
      "field": "name",
      "message": "must not be blank"
    }
  ]
}
```

### Codes de statut HTTP

- `200 OK`: Requête réussie
- `201 Created`: Ressource créée avec succès
- `202 Accepted`: Requête acceptée (pour les opérations asynchrones)
- `204 No Content`: Suppression réussie
- `400 Bad Request`: Données invalides
- `401 Unauthorized`: Non authentifié
- `403 Forbidden`: Non autorisé
- `404 Not Found`: Ressource non trouvée
- `409 Conflict`: Conflit (ex: ressource déjà utilisée)
- `500 Internal Server Error`: Erreur serveur

---

## Sécurité

### Authentification
- JWT (JSON Web Tokens) pour l'authentification
- Token inclus dans le header: `Authorization: Bearer <token>`

### Autorisation
- RBAC (Role-Based Access Control)
- Rôles: ADMIN, USER, VIEWER
- Les credentials sensibles sont chiffrés en base de données

### Rate Limiting
- Limite de 1000 requêtes par heure par utilisateur
- Header de réponse: `X-RateLimit-Remaining`, `X-RateLimit-Reset`

---

## WebSocket pour les mises à jour en temps réel

### Connexion WebSocket
```
ws://api.example.com/ws/jobs
```

### Messages
Lorsqu'un client se connecte au WebSocket, il reçoit des mises à jour en temps réel sur les jobs:

```json
{
  "type": "JOB_STATUS_UPDATE",
  "jobId": "uuid",
  "status": "RUNNING",
  "progress": 45.5,
  "timestamp": "2026-02-12T13:00:00Z"
}
```

Types de messages:
- `JOB_STATUS_UPDATE`: Mise à jour du statut d'un job
- `JOB_LOG`: Nouveau message de log
- `JOB_COMPLETED`: Job terminé
- `JOB_FAILED`: Job échoué

---

## Versioning

L'API utilise le versioning par URL: `/api/v1/...`

Les versions futures seront disponibles via `/api/v2/...`, etc.

---

## Pagination

Toutes les listes supportent la pagination avec les paramètres suivants:
- `page`: numéro de page (commence à 0)
- `size`: nombre d'éléments par page
- `sort`: champ de tri
- `order`: ordre de tri (asc/desc)

Format de réponse paginée:
```json
{
  "content": [...],
  "page": 0,
  "size": 20,
  "totalElements": 150,
  "totalPages": 8
}
```

---

## Technologies recommandées

### Backend (Spring Boot)
- Spring Boot 3.x
- Spring Data JPA (pour la persistence)
- Spring Security (pour l'authentification/autorisation)
- Spring WebSocket (pour les mises à jour temps réel)
- PostgreSQL ou MySQL (base de données principale)
- Redis (pour le cache et les queues)

### Spark
- Apache Spark 3.x
- Spark SQL
- Spark Streaming (pour les pipelines en temps réel)

### Monitoring
- Spring Boot Actuator (health checks, metrics)
- Prometheus (métriques)
- Grafana (visualisation)

---

## Points d'attention pour l'implémentation

1. **Gestion des credentials**: Utiliser un système de chiffrement robuste (ex: AES-256) pour stocker les credentials sensibles
2. **Queue de jobs**: Utiliser un système de queue (ex: Redis Queue, RabbitMQ) pour gérer les exécutions de pipelines
3. **Isolation des exécutions**: Chaque pipeline doit s'exécuter dans un contexte Spark isolé
4. **Gestion des logs**: Les logs doivent être streamés et stockés de manière efficace (ex: Elasticsearch, CloudWatch)
5. **Timeouts**: Implémenter des timeouts pour éviter les exécutions infinies
6. **Retry logic**: Implémenter une stratégie de retry pour les échecs temporaires
7. **Validation des specs**: Valider les spécifications de pipeline avant l'exécution
8. **Monitoring**: Exposer des métriques détaillées sur les exécutions (throughput, latence, etc.)
9. **Backup**: Sauvegarder régulièrement les configurations de pipelines
10. **Audit trail**: Logger toutes les modifications des ressources (projets, pipelines, credentials)

---

## Prochaines étapes

1. Validation de ces spécifications avec l'équipe
2. Création du projet Spring Boot avec les dépendances nécessaires
3. Modélisation de la base de données (schéma SQL)
4. Implémentation des entités JPA
5. Implémentation des repositories
6. Implémentation des services métier
7. Implémentation des controllers REST
8. Implémentation de la sécurité (authentification/autorisation)
9. Implémentation du moteur d'exécution Spark
10. Tests unitaires et d'intégration
11. Documentation API (Swagger/OpenAPI)
12. Déploiement et monitoring
