# Schéma de Base de Données - Spark ETL

## Vue d'ensemble

Ce document décrit le schéma de base de données pour le système Spark ETL.

## Tables

### 1. projects
Stocke les projets de pipeline.

```sql
CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_projects_name UNIQUE (name)
);

CREATE INDEX idx_projects_created_at ON projects(created_at DESC);
```

### 2. credentials
Stocke les informations d'authentification pour les sources et destinations.

```sql
CREATE TABLE credentials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    properties JSONB NOT NULL,
    encrypted BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_credentials_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    CONSTRAINT uk_credentials_project_name UNIQUE (project_id, name)
);

CREATE INDEX idx_credentials_project_id ON credentials(project_id);
CREATE INDEX idx_credentials_type ON credentials(type);
```

**Types de credentials:**
- `DATABASE`: PostgreSQL, MySQL, Oracle, SQL Server
- `S3`: Amazon S3
- `AZURE_BLOB`: Azure Blob Storage
- `GCS`: Google Cloud Storage
- `SFTP`: SFTP Server
- `HTTP`: HTTP/HTTPS endpoint
- `KAFKA`: Apache Kafka

### 3. pipelines
Stocke les définitions de pipeline ETL.

```sql
CREATE TABLE pipelines (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    spec JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pipelines_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    CONSTRAINT uk_pipelines_project_name UNIQUE (project_id, name)
);

CREATE INDEX idx_pipelines_project_id ON pipelines(project_id);
CREATE INDEX idx_pipelines_enabled ON pipelines(enabled);
CREATE INDEX idx_pipelines_created_at ON pipelines(created_at DESC);
```

### 4. jobs
Stocke les exécutions de pipeline.

```sql
CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    progress DECIMAL(5,2) DEFAULT 0.0,
    statistics JSONB,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_jobs_pipeline FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE,
    CONSTRAINT chk_jobs_status CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    CONSTRAINT chk_jobs_progress CHECK (progress >= 0 AND progress <= 100)
);

CREATE INDEX idx_jobs_pipeline_id ON jobs(pipeline_id);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_created_at ON jobs(created_at DESC);
CREATE INDEX idx_jobs_status_created ON jobs(status, created_at DESC);
```

**Statuts de job:**
- `PENDING`: En attente d'exécution
- `RUNNING`: En cours d'exécution
- `COMPLETED`: Terminé avec succès
- `FAILED`: Échoué
- `CANCELLED`: Annulé par l'utilisateur

### 5. job_logs
Stocke les logs d'exécution des jobs.

```sql
CREATE TABLE job_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(10) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB,
    CONSTRAINT fk_job_logs_job FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    CONSTRAINT chk_job_logs_level CHECK (level IN ('DEBUG', 'INFO', 'WARN', 'ERROR'))
);

CREATE INDEX idx_job_logs_job_id ON job_logs(job_id);
CREATE INDEX idx_job_logs_timestamp ON job_logs(timestamp DESC);
CREATE INDEX idx_job_logs_level ON job_logs(level);
CREATE INDEX idx_job_logs_job_timestamp ON job_logs(job_id, timestamp DESC);
```

### 6. audit_logs (optionnel)
Stocke l'historique des modifications pour l'audit.

```sql
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type VARCHAR(50) NOT NULL,
    entity_id UUID NOT NULL,
    action VARCHAR(20) NOT NULL,
    user_id VARCHAR(255),
    changes JSONB,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_audit_action CHECK (action IN ('CREATE', 'UPDATE', 'DELETE'))
);

CREATE INDEX idx_audit_entity ON audit_logs(entity_type, entity_id);
CREATE INDEX idx_audit_timestamp ON audit_logs(timestamp DESC);
CREATE INDEX idx_audit_user ON audit_logs(user_id);
```

### 7. users (pour l'authentification)
Stocke les utilisateurs du système.

```sql
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(255) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'USER',
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    CONSTRAINT chk_users_role CHECK (role IN ('ADMIN', 'USER', 'VIEWER'))
);

CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_email ON users(email);
```

**Rôles:**
- `ADMIN`: Accès complet (création/modification/suppression de tous les projets)
- `USER`: Accès en lecture/écriture sur ses propres projets
- `VIEWER`: Accès en lecture seule

### 8. project_members (optionnel pour le multi-user)
Gère les membres d'un projet et leurs permissions.

```sql
CREATE TABLE project_members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    user_id UUID NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'MEMBER',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_project_members_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    CONSTRAINT fk_project_members_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT uk_project_members UNIQUE (project_id, user_id),
    CONSTRAINT chk_project_members_role CHECK (role IN ('OWNER', 'ADMIN', 'MEMBER', 'VIEWER'))
);

CREATE INDEX idx_project_members_project ON project_members(project_id);
CREATE INDEX idx_project_members_user ON project_members(user_id);
```

---

## Vues utiles

### Vue des jobs avec informations du pipeline et projet
```sql
CREATE VIEW v_jobs_extended AS
SELECT 
    j.id,
    j.pipeline_id,
    p.name AS pipeline_name,
    p.project_id,
    pr.name AS project_name,
    j.status,
    j.started_at,
    j.completed_at,
    j.progress,
    j.statistics,
    j.error_message,
    j.created_at,
    CASE 
        WHEN j.completed_at IS NOT NULL AND j.started_at IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (j.completed_at - j.started_at))
        ELSE NULL 
    END AS duration_seconds
FROM jobs j
INNER JOIN pipelines p ON j.pipeline_id = p.id
INNER JOIN projects pr ON p.project_id = pr.id;
```

### Vue des statistiques de pipeline
```sql
CREATE VIEW v_pipeline_statistics AS
SELECT 
    p.id AS pipeline_id,
    p.name AS pipeline_name,
    p.project_id,
    COUNT(j.id) AS total_executions,
    SUM(CASE WHEN j.status = 'COMPLETED' THEN 1 ELSE 0 END) AS successful_executions,
    SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_executions,
    SUM(CASE WHEN j.status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_executions,
    AVG(CASE 
        WHEN j.status = 'COMPLETED' AND j.completed_at IS NOT NULL AND j.started_at IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (j.completed_at - j.started_at))
        ELSE NULL 
    END) AS avg_duration_seconds,
    MAX(j.created_at) AS last_execution
FROM pipelines p
LEFT JOIN jobs j ON p.id = j.pipeline_id
GROUP BY p.id, p.name, p.project_id;
```

---

## Triggers

### Trigger pour mettre à jour updated_at automatiquement

```sql
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_projects_updated_at BEFORE UPDATE ON projects
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_credentials_updated_at BEFORE UPDATE ON credentials
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_pipelines_updated_at BEFORE UPDATE ON pipelines
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

### Trigger pour logger les modifications (audit)

```sql
CREATE OR REPLACE FUNCTION log_audit()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO audit_logs (entity_type, entity_id, action, changes)
        VALUES (TG_TABLE_NAME, OLD.id, 'DELETE', row_to_json(OLD)::jsonb);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO audit_logs (entity_type, entity_id, action, changes)
        VALUES (TG_TABLE_NAME, NEW.id, 'UPDATE', jsonb_build_object(
            'old', row_to_json(OLD)::jsonb,
            'new', row_to_json(NEW)::jsonb
        ));
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO audit_logs (entity_type, entity_id, action, changes)
        VALUES (TG_TABLE_NAME, NEW.id, 'CREATE', row_to_json(NEW)::jsonb);
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_projects AFTER INSERT OR UPDATE OR DELETE ON projects
    FOR EACH ROW EXECUTE FUNCTION log_audit();

CREATE TRIGGER audit_pipelines AFTER INSERT OR UPDATE OR DELETE ON pipelines
    FOR EACH ROW EXECUTE FUNCTION log_audit();

CREATE TRIGGER audit_credentials AFTER INSERT OR UPDATE OR DELETE ON credentials
    FOR EACH ROW EXECUTE FUNCTION log_audit();
```

---

## Exemples de requêtes utiles

### Obtenir les jobs actifs avec leurs informations
```sql
SELECT 
    j.id,
    pr.name AS project_name,
    p.name AS pipeline_name,
    j.status,
    j.progress,
    j.started_at,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - j.started_at)) AS running_seconds
FROM jobs j
INNER JOIN pipelines p ON j.pipeline_id = p.id
INNER JOIN projects pr ON p.project_id = pr.id
WHERE j.status IN ('PENDING', 'RUNNING')
ORDER BY j.created_at ASC;
```

### Obtenir les statistiques d'un projet
```sql
SELECT 
    pr.name AS project_name,
    COUNT(DISTINCT p.id) AS total_pipelines,
    COUNT(j.id) AS total_jobs,
    SUM(CASE WHEN j.status = 'COMPLETED' THEN 1 ELSE 0 END) AS successful_jobs,
    SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_jobs,
    SUM(CASE WHEN j.status = 'RUNNING' THEN 1 ELSE 0 END) AS running_jobs
FROM projects pr
LEFT JOIN pipelines p ON pr.id = p.project_id
LEFT JOIN jobs j ON p.id = j.pipeline_id
WHERE pr.id = 'project-uuid-here'
GROUP BY pr.id, pr.name;
```

### Obtenir l'historique des exécutions avec pagination
```sql
SELECT 
    j.id,
    p.name AS pipeline_name,
    j.status,
    j.started_at,
    j.completed_at,
    EXTRACT(EPOCH FROM (j.completed_at - j.started_at)) AS duration_seconds,
    j.statistics
FROM jobs j
INNER JOIN pipelines p ON j.pipeline_id = p.id
WHERE p.project_id = 'project-uuid-here'
ORDER BY j.created_at DESC
LIMIT 20 OFFSET 0;
```

### Trouver les pipelines qui n'ont jamais été exécutés
```sql
SELECT 
    p.id,
    p.name,
    p.created_at
FROM pipelines p
LEFT JOIN jobs j ON p.id = j.pipeline_id
WHERE j.id IS NULL
ORDER BY p.created_at DESC;
```

---

## Considérations pour la performance

### Indexation
- Tous les foreign keys ont des index
- Les champs souvent utilisés pour le filtrage ont des index
- Les champs de date pour le tri temporel ont des index

### Partitionnement (pour grande échelle)
Pour les grandes volumétries, considérer le partitionnement de la table `job_logs`:

```sql
-- Partitionnement par mois
CREATE TABLE job_logs (
    id UUID DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(10) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB
) PARTITION BY RANGE (timestamp);

CREATE TABLE job_logs_2026_01 PARTITION OF job_logs
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE job_logs_2026_02 PARTITION OF job_logs
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

-- etc.
```

### Archivage
Créer une stratégie d'archivage pour les anciens jobs et logs:
- Archiver les jobs de plus de 6 mois
- Compresser et archiver les logs de plus de 3 mois
- Conserver uniquement les statistiques agrégées pour l'historique

---

## Sécurité

### Chiffrement des credentials
Les propriétés sensibles dans la colonne `properties` de la table `credentials` doivent être chiffrées:
- Utiliser AES-256 pour le chiffrement
- Stocker les clés de chiffrement dans un gestionnaire de secrets (AWS Secrets Manager, Azure Key Vault, HashiCorp Vault)
- Ne jamais stocker les clés de chiffrement dans le code ou la base de données

### Row-level security (RLS)
Pour PostgreSQL, implémenter la sécurité au niveau des lignes:

```sql
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;

CREATE POLICY project_access_policy ON projects
    FOR ALL
    TO authenticated_user
    USING (
        id IN (
            SELECT project_id 
            FROM project_members 
            WHERE user_id = current_user_id()
        )
    );
```

---

## Scripts de migration

### Script initial (V1__initial_schema.sql)
Créer les tables dans l'ordre suivant:
1. users
2. projects
3. project_members
4. credentials
5. pipelines
6. jobs
7. job_logs
8. audit_logs

### Script d'exemple de données (seed.sql)
```sql
-- Insérer un utilisateur admin
INSERT INTO users (username, email, password_hash, role) 
VALUES ('admin', 'admin@example.com', '$2a$10$...', 'ADMIN');

-- Insérer un projet de démonstration
INSERT INTO projects (name, description) 
VALUES ('Demo Project', 'Projet de démonstration pour tester l''API');

-- etc.
```

---

## Monitoring et métriques

Créer des vues pour le monitoring:

### Métriques en temps réel
```sql
CREATE VIEW v_system_metrics AS
SELECT 
    (SELECT COUNT(*) FROM jobs WHERE status = 'RUNNING') AS running_jobs,
    (SELECT COUNT(*) FROM jobs WHERE status = 'PENDING') AS pending_jobs,
    (SELECT COUNT(*) FROM projects) AS total_projects,
    (SELECT COUNT(*) FROM pipelines WHERE enabled = true) AS active_pipelines,
    (SELECT COUNT(*) FROM users WHERE enabled = true) AS active_users;
```

### Métriques de performance
```sql
CREATE VIEW v_performance_metrics AS
SELECT 
    DATE_TRUNC('hour', j.created_at) AS hour,
    COUNT(*) AS total_jobs,
    AVG(CASE 
        WHEN j.completed_at IS NOT NULL AND j.started_at IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (j.completed_at - j.started_at))
        ELSE NULL 
    END) AS avg_duration_seconds,
    SUM(CASE WHEN j.status = 'COMPLETED' THEN 1 ELSE 0 END) AS successful_jobs,
    SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_jobs
FROM jobs j
WHERE j.created_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;
```
