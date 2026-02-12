# Guide d'implémentation - Spark ETL API

## Table des matières
1. [Structure du projet](#structure-du-projet)
2. [Configuration](#configuration)
3. [Bonnes pratiques](#bonnes-pratiques)
4. [Sécurité](#sécurité)
5. [Gestion des erreurs](#gestion-des-erreurs)
6. [Performance](#performance)
7. [Testing](#testing)

---

## Structure du projet

### Organisation des packages

```
com.example.sparketl/
├── SparkEtlApplication.java              # Point d'entrée Spring Boot
├── config/
│   ├── SecurityConfig.java               # Configuration Spring Security
│   ├── WebSocketConfig.java              # Configuration WebSocket
│   ├── SparkConfig.java                  # Configuration Apache Spark
│   ├── JpaConfig.java                    # Configuration JPA
│   └── RedisConfig.java                  # Configuration Redis
├── controller/
│   ├── ProjectController.java            # Endpoints projets
│   ├── CredentialController.java         # Endpoints credentials
│   ├── PipelineController.java           # Endpoints pipelines
│   ├── JobController.java                # Endpoints jobs
│   └── websocket/
│       └── JobWebSocketController.java   # WebSocket controller
├── dto/
│   ├── request/
│   │   ├── CreateProjectRequest.java
│   │   ├── CreatePipelineRequest.java
│   │   └── ...
│   └── response/
│       ├── ProjectResponse.java
│       ├── PipelineResponse.java
│       └── ...
├── entity/
│   ├── Project.java                      # Entité JPA Project
│   ├── Credential.java                   # Entité JPA Credential
│   ├── Pipeline.java                     # Entité JPA Pipeline
│   ├── Job.java                          # Entité JPA Job
│   └── JobLog.java                       # Entité JPA JobLog
├── exception/
│   ├── ResourceNotFoundException.java
│   ├── ValidationException.java
│   ├── ConflictException.java
│   └── GlobalExceptionHandler.java       # Gestion globale des erreurs
├── mapper/
│   ├── ProjectMapper.java                # Conversion Entity <-> DTO
│   ├── PipelineMapper.java
│   └── ...
├── repository/
│   ├── ProjectRepository.java            # JPA Repository
│   ├── CredentialRepository.java
│   ├── PipelineRepository.java
│   ├── JobRepository.java
│   └── JobLogRepository.java
├── security/
│   ├── JwtTokenProvider.java             # Génération/validation JWT
│   ├── JwtAuthenticationFilter.java      # Filtre JWT
│   └── CustomUserDetailsService.java     # UserDetails Service
├── service/
│   ├── ProjectService.java               # Logique métier projets
│   ├── CredentialService.java            # Logique métier credentials
│   ├── PipelineService.java              # Logique métier pipelines
│   ├── JobService.java                   # Logique métier jobs
│   ├── EncryptionService.java            # Chiffrement credentials
│   └── NotificationService.java          # Notifications WebSocket
└── spark/
    ├── SparkJobExecutor.java             # Exécution jobs Spark
    ├── PipelineSpecParser.java           # Parsing specs pipeline
    ├── source/
    │   ├── DataSourceFactory.java
    │   ├── DatabaseSource.java
    │   ├── S3Source.java
    │   └── ...
    ├── transformation/
    │   ├── TransformationFactory.java
    │   ├── FilterTransformation.java
    │   ├── MapTransformation.java
    │   └── ...
    └── destination/
        ├── DataDestinationFactory.java
        ├── DatabaseDestination.java
        ├── S3Destination.java
        └── ...
```

---

## Configuration

### application.yml

```yaml
spring:
  application:
    name: spark-etl-api
  
  datasource:
    url: jdbc:postgresql://localhost:5432/sparketl
    username: ${DB_USERNAME:sparketl}
    password: ${DB_PASSWORD:password}
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 10
      minimum-idle: 5
  
  jpa:
    hibernate:
      ddl-auto: validate
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        format_sql: true
  
  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: true
  
  redis:
    host: ${REDIS_HOST:localhost}
    port: ${REDIS_PORT:6379}
    password: ${REDIS_PASSWORD:}
  
  security:
    jwt:
      secret: ${JWT_SECRET:changeme}
      expiration: 86400000 # 24 heures

spark:
  master: ${SPARK_MASTER:local[*]}
  app-name: spark-etl
  executor:
    memory: ${SPARK_EXECUTOR_MEMORY:2g}
    cores: ${SPARK_EXECUTOR_CORES:2}
  
encryption:
  key: ${ENCRYPTION_KEY:changeme-must-be-32-chars-long}
  algorithm: AES

job:
  queue:
    max-concurrent: ${MAX_CONCURRENT_JOBS:5}
  timeout:
    default: ${JOB_TIMEOUT:3600} # 1 heure en secondes

logging:
  level:
    com.example.sparketl: DEBUG
    org.springframework: INFO
    org.hibernate: WARN
```

### pom.xml (Maven)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.0</version>
        <relativePath/>
    </parent>
    
    <groupId>com.example</groupId>
    <artifactId>spark-etl-api</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <name>Spark ETL API</name>
    
    <properties>
        <java.version>17</java.version>
        <spark.version>3.5.0</spark.version>
    </properties>
    
    <dependencies>
        <!-- Spring Boot -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-security</artifactId>
        </dependency>
        
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-websocket</artifactId>
        </dependency>
        
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        
        <!-- Database -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        
        <!-- Redis -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis</artifactId>
        </dependency>
        
        <!-- Apache Spark -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.13</artifactId>
            <version>${spark.version}</version>
        </dependency>
        
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.13</artifactId>
            <version>${spark.version}</version>
        </dependency>
        
        <!-- JWT -->
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-api</artifactId>
            <version>0.11.5</version>
        </dependency>
        
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-impl</artifactId>
            <version>0.11.5</version>
            <scope>runtime</scope>
        </dependency>
        
        <dependency>
            <groupId>io.jsonwebtoken</groupId>
            <artifactId>jjwt-jackson</artifactId>
            <version>0.11.5</version>
            <scope>runtime</scope>
        </dependency>
        
        <!-- Utilities -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
        
        <dependency>
            <groupId>org.mapstruct</groupId>
            <artifactId>mapstruct</artifactId>
            <version>1.5.5.Final</version>
        </dependency>
        
        <!-- OpenAPI/Swagger -->
        <dependency>
            <groupId>org.springdoc</groupId>
            <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
            <version>2.2.0</version>
        </dependency>
        
        <!-- Testing -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        
        <dependency>
            <groupId>org.springframework.security</groupId>
            <artifactId>spring-security-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
    
    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

---

## Bonnes pratiques

### 1. Utilisation des DTOs

**Pourquoi?** Séparer les entités JPA des objets exposés par l'API pour:
- Éviter les problèmes de sérialisation (lazy loading)
- Contrôler les données exposées
- Faciliter les évolutions

**Exemple:**

```java
// Entity
@Entity
@Table(name = "projects")
public class Project {
    @Id
    @GeneratedValue
    private UUID id;
    private String name;
    private String description;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    
    @OneToMany(mappedBy = "project")
    private List<Pipeline> pipelines;
}

// DTO Response
public class ProjectResponse {
    private UUID id;
    private String name;
    private String description;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    // Pas de pipelines pour éviter N+1
}

// Mapper avec MapStruct
@Mapper(componentModel = "spring")
public interface ProjectMapper {
    ProjectResponse toResponse(Project project);
    Project toEntity(CreateProjectRequest request);
}
```

### 2. Validation des données

Utiliser les annotations de validation Java:

```java
public class CreatePipelineRequest {
    @NotBlank(message = "Le nom est obligatoire")
    @Size(min = 3, max = 255, message = "Le nom doit contenir entre 3 et 255 caractères")
    private String name;
    
    @Size(max = 1000, message = "La description ne peut pas dépasser 1000 caractères")
    private String description;
    
    @NotNull(message = "La spécification est obligatoire")
    @Valid
    private PipelineSpec spec;
}
```

### 3. Gestion de la pagination

Utiliser Spring Data Pagination:

```java
@GetMapping("/api/v1/projects")
public ResponseEntity<Page<ProjectResponse>> listProjects(
    @RequestParam(defaultValue = "0") int page,
    @RequestParam(defaultValue = "20") int size,
    @RequestParam(defaultValue = "createdAt") String sort,
    @RequestParam(defaultValue = "DESC") Sort.Direction order
) {
    Pageable pageable = PageRequest.of(page, size, Sort.by(order, sort));
    Page<Project> projects = projectService.findAll(pageable);
    return ResponseEntity.ok(projects.map(projectMapper::toResponse));
}
```

### 4. Gestion transactionnelle

```java
@Service
@Transactional
public class PipelineService {
    
    @Transactional(readOnly = true)
    public List<Pipeline> findAll() {
        return pipelineRepository.findAll();
    }
    
    @Transactional
    public Pipeline create(CreatePipelineRequest request) {
        // Logique de création avec gestion transactionnelle automatique
    }
}
```

---

## Sécurité

### 1. Chiffrement des credentials

```java
@Service
public class EncryptionService {
    
    @Value("${encryption.key}")
    private String encryptionKey;
    
    public String encrypt(String plainText) {
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            SecretKeySpec keySpec = new SecretKeySpec(
                encryptionKey.getBytes(StandardCharsets.UTF_8), "AES"
            );
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
            byte[] encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors du chiffrement", e);
        }
    }
    
    public String decrypt(String encryptedText) {
        // Implémentation du déchiffrement
    }
}
```

### 2. Configuration Spring Security

```java
@Configuration
@EnableWebSecurity
public class SecurityConfig {
    
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf().disable()
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/v1/auth/**").permitAll()
                .requestMatchers("/api/v1/admin/**").hasRole("ADMIN")
                .anyRequest().authenticated()
            )
            .sessionManagement()
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
            .and()
            .addFilterBefore(jwtAuthenticationFilter(), 
                UsernamePasswordAuthenticationFilter.class);
        
        return http.build();
    }
}
```

### 3. Validation des permissions

```java
@Service
public class ProjectService {
    
    public Project findById(UUID id) {
        Project project = projectRepository.findById(id)
            .orElseThrow(() -> new ResourceNotFoundException("Projet non trouvé"));
        
        // Vérifier que l'utilisateur a accès à ce projet
        if (!hasAccess(getCurrentUser(), project)) {
            throw new AccessDeniedException("Accès refusé");
        }
        
        return project;
    }
}
```

---

## Gestion des erreurs

### GlobalExceptionHandler

```java
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(ResourceNotFoundException ex) {
        ErrorResponse error = new ErrorResponse(
            LocalDateTime.now(),
            HttpStatus.NOT_FOUND.value(),
            "Not Found",
            ex.getMessage(),
            null
        );
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
    }
    
    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ErrorResponse> handleValidation(ValidationException ex) {
        ErrorResponse error = new ErrorResponse(
            LocalDateTime.now(),
            HttpStatus.BAD_REQUEST.value(),
            "Bad Request",
            ex.getMessage(),
            ex.getErrors()
        );
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }
    
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentNotValid(
        MethodArgumentNotValidException ex
    ) {
        List<FieldError> errors = ex.getBindingResult().getFieldErrors()
            .stream()
            .map(error -> new FieldError(error.getField(), error.getDefaultMessage()))
            .collect(Collectors.toList());
        
        ErrorResponse errorResponse = new ErrorResponse(
            LocalDateTime.now(),
            HttpStatus.BAD_REQUEST.value(),
            "Validation Failed",
            "Les données fournies sont invalides",
            errors
        );
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }
}
```

---

## Performance

### 1. Éviter les N+1 queries

```java
// Mauvais
public List<Pipeline> findAllWithProjects() {
    return pipelineRepository.findAll(); // N+1 si on accède au project
}

// Bon
public interface PipelineRepository extends JpaRepository<Pipeline, UUID> {
    @Query("SELECT p FROM Pipeline p JOIN FETCH p.project")
    List<Pipeline> findAllWithProjects();
}
```

### 2. Utiliser le cache

```java
@Service
public class ProjectService {
    
    @Cacheable(value = "projects", key = "#id")
    public Project findById(UUID id) {
        return projectRepository.findById(id)
            .orElseThrow(() -> new ResourceNotFoundException("Projet non trouvé"));
    }
    
    @CacheEvict(value = "projects", key = "#project.id")
    public Project update(Project project) {
        return projectRepository.save(project);
    }
}
```

### 3. Optimiser les requêtes

```java
// Utiliser les projections pour ne récupérer que les champs nécessaires
public interface JobRepository extends JpaRepository<Job, UUID> {
    
    @Query("SELECT new com.example.dto.JobSummary(j.id, j.status, j.progress) " +
           "FROM Job j WHERE j.status IN :statuses")
    List<JobSummary> findSummaryByStatus(@Param("statuses") List<JobStatus> statuses);
}
```

---

## Testing

### 1. Tests unitaires

```java
@ExtendWith(MockitoExtension.class)
class ProjectServiceTest {
    
    @Mock
    private ProjectRepository projectRepository;
    
    @InjectMocks
    private ProjectService projectService;
    
    @Test
    void testCreateProject() {
        // Given
        CreateProjectRequest request = new CreateProjectRequest("Test Project", "Description");
        Project project = new Project();
        project.setName(request.getName());
        
        when(projectRepository.save(any(Project.class))).thenReturn(project);
        
        // When
        Project result = projectService.create(request);
        
        // Then
        assertNotNull(result);
        assertEquals("Test Project", result.getName());
        verify(projectRepository, times(1)).save(any(Project.class));
    }
}
```

### 2. Tests d'intégration

```java
@SpringBootTest
@AutoConfigureMockMvc
class ProjectControllerIntegrationTest {
    
    @Autowired
    private MockMvc mockMvc;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Test
    void testCreateProject() throws Exception {
        CreateProjectRequest request = new CreateProjectRequest("Test", "Description");
        
        mockMvc.perform(post("/api/v1/projects")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Test"));
    }
}
```

### 3. Tests de sécurité

```java
@SpringBootTest
@AutoConfigureMockMvc
class SecurityTest {
    
    @Autowired
    private MockMvc mockMvc;
    
    @Test
    void testUnauthorizedAccess() throws Exception {
        mockMvc.perform(get("/api/v1/projects"))
                .andExpect(status().isUnauthorized());
    }
    
    @Test
    @WithMockUser(roles = "USER")
    void testAuthorizedAccess() throws Exception {
        mockMvc.perform(get("/api/v1/projects"))
                .andExpect(status().isOk());
    }
}
```

---

## Monitoring et Observabilité

### 1. Spring Boot Actuator

Activer les endpoints de monitoring:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
```

### 2. Custom Metrics

```java
@Service
public class JobService {
    
    private final Counter jobsExecutedCounter;
    private final Timer jobExecutionTimer;
    
    public JobService(MeterRegistry meterRegistry) {
        this.jobsExecutedCounter = meterRegistry.counter("jobs.executed");
        this.jobExecutionTimer = meterRegistry.timer("jobs.execution.time");
    }
    
    public Job execute(UUID pipelineId) {
        return jobExecutionTimer.record(() -> {
            Job job = doExecute(pipelineId);
            jobsExecutedCounter.increment();
            return job;
        });
    }
}
```

---

## Logging

### Configuration Logback

```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/spark-etl.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>logs/spark-etl.%d{yyyy-MM-dd}.log</fileNamePattern>
            <maxHistory>30</maxHistory>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <root level="INFO">
        <appender-ref ref="STDOUT" />
        <appender-ref ref="FILE" />
    </root>
</configuration>
```

### Structured Logging

```java
@Slf4j
@Service
public class PipelineService {
    
    public Pipeline create(CreatePipelineRequest request) {
        log.info("Creating pipeline: name={}, projectId={}", 
            request.getName(), request.getProjectId());
        
        try {
            Pipeline pipeline = doCreate(request);
            log.info("Pipeline created successfully: id={}", pipeline.getId());
            return pipeline;
        } catch (Exception e) {
            log.error("Failed to create pipeline: name={}, error={}", 
                request.getName(), e.getMessage(), e);
            throw e;
        }
    }
}
```

---

## Conclusion

Ce guide fournit les bases pour implémenter l'API Spark ETL de manière robuste et maintenable. Les principes clés sont:

1. **Séparation des responsabilités** - Controllers, Services, Repositories
2. **Sécurité par défaut** - Authentification, autorisation, chiffrement
3. **Performance** - Pagination, cache, optimisation des requêtes
4. **Observabilité** - Logging, métriques, monitoring
5. **Tests** - Unitaires, intégration, sécurité

Pour toute question ou clarification, se référer aux spécifications complètes dans [API_SPECIFICATIONS.md](./API_SPECIFICATIONS.md) et [DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md).
