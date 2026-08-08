# Java Worker Phase 1 (Shell-Only) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A standalone Java execution worker that claims and runs `shell` tasks against the distributed backends (PostgreSQL + ClickHouse server), streaming logs to `task_logs`, with no Object/View support.

**Architecture:** Single deployable jar under `java/aaiclick-worker`. The worker ports the Python worker's SQL contract verbatim (claim CTE with `FOR UPDATE SKIP LOCKED`, heartbeats, `run_epoch`-fenced status writes), runs the task argv with `ProcessBuilder`, and reports failure as `PENDING_CLEANUP` so the Python `BackgroundWorker` keeps sole ownership of retries and cleanup. ClickHouse is touched only over HTTP: `SELECT generateSnowflakeID()` for IDs and `INSERT ... FORMAT JSONEachRow` for logs.

**Tech Stack:** Java 21, Maven, PostgreSQL JDBC (`org.postgresql:postgresql`), Jackson (`com.fasterxml.jackson.core:jackson-databind`), Java 11+ built-in `java.net.http.HttpClient`, JUnit 5, Testcontainers (`postgresql`, `clickhouse`).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-08-java-worker-design.md`.
- Distributed-only: the worker MUST refuse to start when `AAICLICK_SQL_URL` starts with `sqlite` or `AAICLICK_CH_URL` starts with `chdb://`.
- Claim filter: `entry_type = 'shell' AND image_source IS NULL` — never claim module tasks or container-bound shell tasks.
- Status strings MUST match `aaiclick/orchestration/models.py` exactly: tasks `PENDING/CLAIMED/RUNNING/COMPLETED/FAILED/CANCELLED/PENDING_CLEANUP/UPSTREAM_FAILED`; jobs `PENDING/RUNNING/COMPLETED/FAILED/CANCELLED`; workers `ACTIVE/IDLE/STOPPING/STOPPED`.
- Failure path: the worker only sets `PENDING_CLEANUP` (+ error). It NEVER implements retry/backoff — that is the Python `BackgroundWorker`'s job.
- Every task-row write after claim is fenced: `WHERE run_epoch = <epoch read at claim>`.
- All timestamps are UTC.
- Java code style: no wildcard imports, `record` for value types, one class per file.
- Maven groupId `io.github.kolodkin`, artifact `aaiclick-worker`, version `0.0.1-SNAPSHOT` (release wiring is out of scope for phase 1 tasks except CI build).
- Testcontainers images: `postgres:16`, `clickhouse/clickhouse-server:24.8`.

---

### Task 1: Maven skeleton + Config

**Files:**
- Create: `java/pom.xml` (parent)
- Create: `java/aaiclick-worker/pom.xml`
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/config/WorkerConfig.java`
- Test: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/config/WorkerConfigTest.java`

**Interfaces:**
- Produces: `WorkerConfig.fromEnv(Map<String,String> env)` and the record
  `WorkerConfig(String jdbcUrl, String dbUser, String dbPassword, String chHttpUrl, String chUser, String chPassword, String chDatabase, Double taskTimeoutSeconds)`.
  Every later task consumes `WorkerConfig`.

- [ ] **Step 1: Write the parent POM**

`java/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>io.github.kolodkin</groupId>
  <artifactId>aaiclick-parent</artifactId>
  <version>0.0.1-SNAPSHOT</version>
  <packaging>pom</packaging>
  <modules>
    <module>aaiclick-worker</module>
  </modules>
  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <junit.version>5.10.2</junit.version>
    <testcontainers.version>1.20.4</testcontainers.version>
  </properties>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>42.7.4</version>
      </dependency>
      <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>2.18.2</version>
      </dependency>
      <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>${junit.version}</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>postgresql</artifactId>
        <version>${testcontainers.version}</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>clickhouse</artifactId>
        <version>${testcontainers.version}</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>${testcontainers.version}</version>
        <scope>test</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.2.5</version>
      </plugin>
    </plugins>
  </build>
</project>
```

`java/aaiclick-worker/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>io.github.kolodkin</groupId>
    <artifactId>aaiclick-parent</artifactId>
    <version>0.0.1-SNAPSHOT</version>
  </parent>
  <artifactId>aaiclick-worker</artifactId>
  <dependencies>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>postgresql</artifactId>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>clickhouse</artifactId>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>junit-jupiter</artifactId>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.3</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>io.aaiclick.worker.Worker</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Write the failing config test**

`WorkerConfigTest.java`:

```java
package io.aaiclick.worker.config;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerConfigTest {

    private static final Map<String, String> DISTRIBUTED = Map.of(
        "AAICLICK_SQL_URL", "postgresql+asyncpg://app:secret@pg.example:5432/aaiclick",
        "AAICLICK_CH_URL", "clickhouse://chuser:chpass@ch.example:8123/default"
    );

    @Test
    void parsesDistributedUrls() {
        WorkerConfig cfg = WorkerConfig.fromEnv(DISTRIBUTED);
        assertEquals("jdbc:postgresql://pg.example:5432/aaiclick", cfg.jdbcUrl());
        assertEquals("app", cfg.dbUser());
        assertEquals("secret", cfg.dbPassword());
        assertEquals("http://ch.example:8123", cfg.chHttpUrl());
        assertEquals("chuser", cfg.chUser());
        assertEquals("chpass", cfg.chPassword());
        assertEquals("default", cfg.chDatabase());
        assertNull(cfg.taskTimeoutSeconds());
    }

    @Test
    void defaultsChPortAndDatabase() {
        WorkerConfig cfg = WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "postgresql+asyncpg://app:secret@pg:5432/aaiclick",
            "AAICLICK_CH_URL", "clickhouse://ch.example"
        ));
        assertEquals("http://ch.example:8123", cfg.chHttpUrl());
        assertEquals("default", cfg.chDatabase());
    }

    @Test
    void parsesTaskTimeout() {
        java.util.HashMap<String, String> env = new java.util.HashMap<>(DISTRIBUTED);
        env.put("AAICLICK_TASK_TIMEOUT", "12.5");
        assertEquals(12.5, WorkerConfig.fromEnv(env).taskTimeoutSeconds());
    }

    @Test
    void rejectsLocalBackends() {
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "sqlite+aiosqlite:///home/u/.aaiclick/local.db",
            "AAICLICK_CH_URL", "clickhouse://ch:8123/default"
        )));
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "postgresql+asyncpg://a:b@pg:5432/db",
            "AAICLICK_CH_URL", "chdb:///home/u/.aaiclick/chdb_data"
        )));
    }

    @Test
    void rejectsMissingUrls() {
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnv(Map.of()));
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=WorkerConfigTest`
Expected: COMPILATION ERROR — `WorkerConfig` does not exist.

- [ ] **Step 4: Implement `WorkerConfig`**

```java
package io.aaiclick.worker.config;

import java.net.URI;
import java.util.Map;

/** Environment-driven configuration, mirroring aaiclick/backend.py URL formats.
 *
 * AAICLICK_SQL_URL: postgresql+asyncpg://user:pass@host:port/db
 * AAICLICK_CH_URL:  clickhouse://user:pass@host:port/db (port defaults to 8123,
 * the HTTP interface; db defaults to "default").
 */
public record WorkerConfig(
    String jdbcUrl,
    String dbUser,
    String dbPassword,
    String chHttpUrl,
    String chUser,
    String chPassword,
    String chDatabase,
    Double taskTimeoutSeconds
) {

    public static WorkerConfig fromEnv(Map<String, String> env) {
        String sqlUrl = require(env, "AAICLICK_SQL_URL");
        String chUrl = require(env, "AAICLICK_CH_URL");
        if (sqlUrl.startsWith("sqlite")) {
            throw new IllegalArgumentException(
                "AAICLICK_SQL_URL is sqlite — the Java worker is distributed-only; point it at PostgreSQL");
        }
        if (chUrl.startsWith("chdb://")) {
            throw new IllegalArgumentException(
                "AAICLICK_CH_URL is chdb — the Java worker is distributed-only; point it at a ClickHouse server");
        }

        URI sql = URI.create(sqlUrl.replaceFirst("^postgresql\\+[a-z0-9]+", "postgresql"));
        String[] sqlUserInfo = splitUserInfo(sql.getUserInfo());
        String jdbc = "jdbc:postgresql://" + sql.getHost() + ":" + (sql.getPort() == -1 ? 5432 : sql.getPort())
            + sql.getPath();

        URI ch = URI.create(chUrl);
        String[] chUserInfo = splitUserInfo(ch.getUserInfo());
        String chHttp = "http://" + ch.getHost() + ":" + (ch.getPort() == -1 ? 8123 : ch.getPort());
        String chDb = (ch.getPath() == null || ch.getPath().isEmpty() || ch.getPath().equals("/"))
            ? "default" : ch.getPath().substring(1);

        String rawTimeout = env.get("AAICLICK_TASK_TIMEOUT");
        Double timeout = (rawTimeout == null || rawTimeout.isEmpty()) ? null : Double.parseDouble(rawTimeout);

        return new WorkerConfig(jdbc, sqlUserInfo[0], sqlUserInfo[1], chHttp, chUserInfo[0], chUserInfo[1], chDb,
            timeout);
    }

    private static String require(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(key + " must be set for the Java worker");
        }
        return value;
    }

    private static String[] splitUserInfo(String userInfo) {
        if (userInfo == null) {
            return new String[] {"", ""};
        }
        int colon = userInfo.indexOf(':');
        return colon == -1
            ? new String[] {userInfo, ""}
            : new String[] {userInfo.substring(0, colon), userInfo.substring(colon + 1)};
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=WorkerConfigTest`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add java/
git commit -m "java-worker: maven skeleton and env config parsing"
```

---

### Task 2: ClickHouse HTTP client + snowflake IDs

**Files:**
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/ch/ChClient.java`
- Test: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/ch/ChClientTest.java`

**Interfaces:**
- Consumes: `WorkerConfig` (Task 1).
- Produces: `new ChClient(WorkerConfig cfg)`, `String query(String sql)` (returns raw TSV body, trailing newline stripped), `void insertJsonEachRow(String table, List<ObjectNode> rows)`, `long nextSnowflakeId()`.

- [ ] **Step 1: Write the failing Testcontainers test**

```java
package io.aaiclick.worker.ch;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.aaiclick.worker.config.WorkerConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class ChClientTest {

    @Container
    static final ClickHouseContainer CH = new ClickHouseContainer("clickhouse/clickhouse-server:24.8");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ChClient client() {
        WorkerConfig cfg = WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "postgresql+asyncpg://x:x@unused:5432/x",
            "AAICLICK_CH_URL", "clickhouse://" + CH.getUsername() + ":" + CH.getPassword()
                + "@" + CH.getHost() + ":" + CH.getMappedPort(8123) + "/default"
        ));
        return new ChClient(cfg);
    }

    @Test
    void queryReturnsScalar() {
        assertEquals("2", client().query("SELECT 1 + 1"));
    }

    @Test
    void nextSnowflakeIdIsPositiveAndIncreasing() {
        ChClient ch = client();
        long first = ch.nextSnowflakeId();
        long second = ch.nextSnowflakeId();
        assertTrue(first > 0);
        assertTrue(second > first);
    }

    @Test
    void insertJsonEachRowRoundTrips() {
        ChClient ch = client();
        ch.query("CREATE TABLE IF NOT EXISTS jer_test (a UInt64, b String) ENGINE = MergeTree() ORDER BY a");
        ObjectNode row = MAPPER.createObjectNode();
        row.put("a", 7L);
        row.put("b", "hello");
        ch.insertJsonEachRow("jer_test", List.of(row));
        assertEquals("hello", ch.query("SELECT b FROM jer_test WHERE a = 7"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=ChClientTest`
Expected: COMPILATION ERROR — `ChClient` does not exist.

- [ ] **Step 3: Implement `ChClient`**

```java
package io.aaiclick.worker.ch;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.aaiclick.worker.config.WorkerConfig;

/** Minimal ClickHouse HTTP client: scalar queries and JSONEachRow inserts.
 *
 * The worker's only ClickHouse touchpoints are generateSnowflakeID() and
 * task_logs inserts — no Object/View support by design.
 */
public class ChClient {

    private final HttpClient http = HttpClient.newHttpClient();
    private final String baseUrl;
    private final String user;
    private final String password;
    private final String database;

    public ChClient(WorkerConfig cfg) {
        this.baseUrl = cfg.chHttpUrl();
        this.user = cfg.chUser();
        this.password = cfg.chPassword();
        this.database = cfg.chDatabase();
    }

    public String query(String sql) {
        return post(sql, "");
    }

    public void insertJsonEachRow(String table, List<ObjectNode> rows) {
        if (rows.isEmpty()) {
            return;
        }
        String body = rows.stream().map(ObjectNode::toString).collect(Collectors.joining("\n"));
        post("INSERT INTO " + table + " FORMAT JSONEachRow", body);
    }

    public long nextSnowflakeId() {
        return Long.parseLong(query("SELECT generateSnowflakeID()"));
    }

    private String post(String sql, String body) {
        String url = baseUrl + "/?database=" + URLEncoder.encode(database, StandardCharsets.UTF_8)
            + "&query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8);
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        if (!user.isEmpty()) {
            builder.header("X-ClickHouse-User", user).header("X-ClickHouse-Key", password);
        }
        try {
            HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new ChException("ClickHouse HTTP " + response.statusCode() + ": " + response.body());
            }
            String text = response.body();
            return text.endsWith("\n") ? text.substring(0, text.length() - 1) : text;
        } catch (IOException e) {
            throw new ChException("ClickHouse request failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ChException("ClickHouse request interrupted", e);
        }
    }

    public static class ChException extends RuntimeException {
        public ChException(String message) { super(message); }
        public ChException(String message, Throwable cause) { super(message, cause); }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=ChClientTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add java/aaiclick-worker
git commit -m "java-worker: ClickHouse HTTP client and snowflake IDs"
```

---

### Task 3: Postgres test harness + schema fixture

**Files:**
- Create: `java/aaiclick-worker/src/test/resources/schema.sql`
- Create: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/db/PgTestBase.java`
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/db/Db.java`
- Test: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/db/DbTest.java`

**Interfaces:**
- Produces: `Db` — `new Db(WorkerConfig cfg)`, `Connection connect()` (autocommit ON; callers own transactions when needed).
- Produces (test-only): `PgTestBase` — extends into repo tests; provides `static Db db()` and a fresh schema per class.

The fixture DDL covers only the columns the worker touches, matching `aaiclick/orchestration/models.py`. Drift guard is the cross-language CI test (Task 9), which runs against a Python-migrated schema.

- [ ] **Step 1: Write the schema fixture**

`schema.sql`:

```sql
CREATE TABLE execution_workers (
    id BIGINT PRIMARY KEY,
    hostname VARCHAR NOT NULL,
    pid INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP NOT NULL,
    last_heartbeat TIMESTAMP NOT NULL,
    tasks_completed INTEGER NOT NULL DEFAULT 0,
    tasks_failed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE jobs (
    id BIGINT PRIMARY KEY,
    name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    run_type VARCHAR NOT NULL DEFAULT 'MANUAL',
    preservation_mode VARCHAR NOT NULL DEFAULT 'NONE',
    runner_mode VARCHAR NOT NULL DEFAULT 'subprocess',
    runner JSON,
    created_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error VARCHAR
);

CREATE TABLE groups (
    id BIGINT PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    parent_group_id BIGINT,
    name VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE tasks (
    id BIGINT PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id),
    group_id BIGINT REFERENCES groups(id),
    entrypoint VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    kwargs JSON NOT NULL DEFAULT '{}',
    entry_type VARCHAR,
    command JSON,
    command_env JSON,
    image_source JSON,
    is_image_build BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    claimed_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    execution_worker_id BIGINT REFERENCES execution_workers(id),
    result JSON,
    error VARCHAR,
    max_retries INTEGER NOT NULL DEFAULT 0,
    attempt INTEGER NOT NULL DEFAULT 0,
    retry_after TIMESTAMP,
    run_ids JSON NOT NULL DEFAULT '[]',
    run_statuses JSON NOT NULL DEFAULT '[]',
    run_epoch BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE dependencies (
    previous_id BIGINT NOT NULL,
    previous_type VARCHAR NOT NULL,
    next_id BIGINT NOT NULL,
    next_type VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (previous_id, previous_type, next_id, next_type)
);
```

- [ ] **Step 2: Write the failing `Db` test + harness**

`PgTestBase.java`:

```java
package io.aaiclick.worker.db;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.aaiclick.worker.config.WorkerConfig;

@Testcontainers
public abstract class PgTestBase {

    @Container
    protected static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16");

    protected static Db db() {
        WorkerConfig cfg = new WorkerConfig(
            PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(),
            "http://unused:8123", "", "", "default", null);
        return new Db(cfg);
    }

    @BeforeAll
    static void loadSchema() throws SQLException, IOException {
        String ddl = new String(
            PgTestBase.class.getResourceAsStream("/schema.sql").readAllBytes(), StandardCharsets.UTF_8);
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;");
            stmt.execute(ddl);
        }
    }
}
```

`DbTest.java`:

```java
package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DbTest extends PgTestBase {

    @Test
    void connectsAndQueries() throws SQLException {
        try (Connection conn = db().connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tasks")) {
            rs.next();
            assertEquals(0, rs.getInt(1));
        }
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=DbTest`
Expected: COMPILATION ERROR — `Db` does not exist.

- [ ] **Step 4: Implement `Db`**

```java
package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import io.aaiclick.worker.config.WorkerConfig;

/** JDBC connection factory for the orchestration PostgreSQL database. */
public class Db {

    private final String jdbcUrl;
    private final Properties props;

    public Db(WorkerConfig cfg) {
        this.jdbcUrl = cfg.jdbcUrl();
        this.props = new Properties();
        props.setProperty("user", cfg.dbUser());
        props.setProperty("password", cfg.dbPassword());
    }

    public Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, props);
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=DbTest`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add java/aaiclick-worker
git commit -m "java-worker: JDBC factory, Postgres test harness, schema fixture"
```

---

### Task 4: Worker registration, heartbeat, counters

**Files:**
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/db/WorkerRepo.java`
- Test: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/db/WorkerRepoTest.java`

**Interfaces:**
- Consumes: `Db` (Task 3); caller supplies snowflake ids (from `ChClient.nextSnowflakeId()`; tests pass literals).
- Produces: `WorkerRepo(Db db)` with:
  - `void register(long workerId, String hostname, int pid)` — inserts row with status `ACTIVE`, all timestamps now.
  - `String heartbeat(long workerId)` — sets `last_heartbeat = now()`; sets status to `ACTIVE` unless it is `STOPPING`; returns the post-update status, or `null` if the row is gone. (Mirrors `execution_worker_heartbeat()` in `execution_worker.py`.)
  - `void markStopped(long workerId)` — sets status `STOPPED`.
  - `void bumpCompleted(long workerId)` / `void bumpFailed(long workerId)` — increment counters.

- [ ] **Step 1: Write the failing test**

```java
package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class WorkerRepoTest extends PgTestBase {

    @Test
    void registerHeartbeatStopLifecycle() throws SQLException {
        WorkerRepo repo = new WorkerRepo(db());
        repo.register(101L, "host-a", 4242);
        assertEquals("ACTIVE", repo.heartbeat(101L));

        // an external `execution-worker stop` sets STOPPING; heartbeat must not undo it
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE execution_workers SET status = 'STOPPING' WHERE id = 101");
        }
        assertEquals("STOPPING", repo.heartbeat(101L));

        repo.markStopped(101L);
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT status FROM execution_workers WHERE id = 101")) {
            rs.next();
            assertEquals("STOPPED", rs.getString(1));
        }
    }

    @Test
    void heartbeatOnMissingWorkerReturnsNull() throws SQLException {
        assertNull(new WorkerRepo(db()).heartbeat(999L));
    }

    @Test
    void countersIncrement() throws SQLException {
        WorkerRepo repo = new WorkerRepo(db());
        repo.register(102L, "host-b", 1);
        repo.bumpCompleted(102L);
        repo.bumpCompleted(102L);
        repo.bumpFailed(102L);
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT tasks_completed, tasks_failed FROM execution_workers WHERE id = 102")) {
            rs.next();
            assertEquals(2, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=WorkerRepoTest`
Expected: COMPILATION ERROR — `WorkerRepo` does not exist.

- [ ] **Step 3: Implement `WorkerRepo`**

```java
package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/** execution_workers row lifecycle: register, heartbeat, counters, stop. */
public class WorkerRepo {

    private final Db db;

    public WorkerRepo(Db db) {
        this.db = db;
    }

    public void register(long workerId, String hostname, int pid) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO execution_workers"
                 + " (id, hostname, pid, status, created_at, started_at, last_heartbeat,"
                 + "  tasks_completed, tasks_failed)"
                 + " VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?, 0, 0)")) {
            Timestamp now = Timestamp.from(Instant.now());
            stmt.setLong(1, workerId);
            stmt.setString(2, hostname);
            stmt.setInt(3, pid);
            stmt.setTimestamp(4, now);
            stmt.setTimestamp(5, now);
            stmt.setTimestamp(6, now);
            stmt.executeUpdate();
        }
    }

    public String heartbeat(long workerId) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "UPDATE execution_workers"
                 + " SET last_heartbeat = ?,"
                 + "     status = CASE WHEN status = 'STOPPING' THEN status ELSE 'ACTIVE' END"
                 + " WHERE id = ? RETURNING status")) {
            stmt.setTimestamp(1, Timestamp.from(Instant.now()));
            stmt.setLong(2, workerId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    public void markStopped(long workerId) throws SQLException {
        execute("UPDATE execution_workers SET status = 'STOPPED' WHERE id = ?", workerId);
    }

    public void bumpCompleted(long workerId) throws SQLException {
        execute("UPDATE execution_workers SET tasks_completed = tasks_completed + 1 WHERE id = ?", workerId);
    }

    public void bumpFailed(long workerId) throws SQLException {
        execute("UPDATE execution_workers SET tasks_failed = tasks_failed + 1 WHERE id = ?", workerId);
    }

    private void execute(String sql, long workerId) throws SQLException {
        try (Connection conn = db.connect(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, workerId);
            stmt.executeUpdate();
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=WorkerRepoTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add java/aaiclick-worker
git commit -m "java-worker: registration, heartbeat, and counters"
```

---

### Task 5: Task claiming (ported CTE + capability filter)

**Files:**
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/db/ClaimedTask.java`
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/db/TaskRepo.java`
- Create: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/db/Fixtures.java`
- Test: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/db/TaskRepoClaimTest.java`

**Interfaces:**
- Consumes: `Db` (Task 3).
- Produces:
  - `record ClaimedTask(long id, long jobId, String name, List<String> command, Map<String,String> commandEnv, long runEpoch)`.
  - `TaskRepo(Db db)` with `ClaimedTask claimNext(long workerId)` — returns `null` when nothing is claimable.
  - (test-only) `Fixtures.insertJob(Db, long id, String status)`, `Fixtures.insertShellTask(Db, long id, long jobId, String status, String commandJson)`, `Fixtures.insertModuleTask(Db, long id, long jobId)`, `Fixtures.insertDependency(Db, long prevId, String prevType, long nextId, String nextType)` — plain JDBC INSERT helpers used by Tasks 5, 6, and 8.

The SQL is `PgDbHandler.claim_next_task()` from `aaiclick/orchestration/execution/pg_handler.py` with `DEPENDENCY_WHERE` from `db_handler.py` ported verbatim, plus two capability predicates. Claiming sets status directly to `RUNNING` (matching `claimed_status=TASK_RUNNING` in the Python CTE).

- [ ] **Step 1: Write `Fixtures`**

```java
package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/** Plain JDBC insert helpers for claim/lifecycle tests. */
public final class Fixtures {

    private Fixtures() {}

    public static void insertJob(Db db, long id, String status) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO jobs (id, name, status, created_at) VALUES (?, ?, ?, ?)")) {
            stmt.setLong(1, id);
            stmt.setString(2, "job-" + id);
            stmt.setString(3, status);
            stmt.setTimestamp(4, Timestamp.from(Instant.now()));
            stmt.executeUpdate();
        }
    }

    public static void insertShellTask(Db db, long id, long jobId, String status, String commandJson)
            throws SQLException {
        insertTask(db, id, jobId, status, "shell", commandJson);
    }

    public static void insertModuleTask(Db db, long id, long jobId) throws SQLException {
        insertTask(db, id, jobId, "PENDING", "module", null);
    }

    private static void insertTask(Db db, long id, long jobId, String status, String entryType, String commandJson)
            throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO tasks (id, job_id, entrypoint, name, kwargs, entry_type, command, status, created_at)"
                 + " VALUES (?, ?, '', ?, '{}', ?, ?::json, ?, ?)")) {
            stmt.setLong(1, id);
            stmt.setLong(2, jobId);
            stmt.setString(3, "task-" + id);
            stmt.setString(4, entryType);
            stmt.setString(5, commandJson);
            stmt.setString(6, status);
            stmt.setTimestamp(7, Timestamp.from(Instant.now()));
            stmt.executeUpdate();
        }
    }

    public static void insertDependency(Db db, long prevId, String prevType, long nextId, String nextType)
            throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO dependencies (previous_id, previous_type, next_id, next_type, created_at)"
                 + " VALUES (?, ?, ?, ?, ?)")) {
            stmt.setLong(1, prevId);
            stmt.setString(2, prevType);
            stmt.setLong(3, nextId);
            stmt.setString(4, nextType);
            stmt.setTimestamp(5, Timestamp.from(Instant.now()));
            stmt.executeUpdate();
        }
    }
}
```

- [ ] **Step 2: Write the failing claim test**

```java
package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TaskRepoClaimTest extends PgTestBase {

    @BeforeEach
    void clean() throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM dependencies; DELETE FROM tasks; DELETE FROM groups;"
                + " DELETE FROM jobs; DELETE FROM execution_workers;");
        }
    }

    @Test
    void claimsOldestEligibleShellTask() throws SQLException {
        WorkerRepo workers = new WorkerRepo(db());
        workers.register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");

        ClaimedTask claimed = new TaskRepo(db()).claimNext(1L);
        assertEquals(100L, claimed.id());
        assertEquals(java.util.List.of("echo", "hi"), claimed.command());
        assertEquals(0L, claimed.runEpoch());

        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT t.status, t.execution_worker_id, j.status FROM tasks t"
                 + " JOIN jobs j ON j.id = t.job_id WHERE t.id = 100")) {
            rs.next();
            assertEquals("RUNNING", rs.getString(1));
            assertEquals(1L, rs.getLong(2));
            assertEquals("RUNNING", rs.getString(3));  // claim transitions job PENDING -> RUNNING
        }
    }

    @Test
    void skipsModuleTasks() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertModuleTask(db(), 100L, 10L);
        assertNull(new TaskRepo(db()).claimNext(1L));
    }

    @Test
    void skipsContainerShellTasks() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET image_source = '{\"kind\": \"prebuilt\"}'::json WHERE id = 100");
        }
        assertNull(new TaskRepo(db()).claimNext(1L));
    }

    @Test
    void respectsTaskDependencies() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"up\"]");
        Fixtures.insertShellTask(db(), 101L, 10L, "PENDING", "[\"echo\", \"down\"]");
        Fixtures.insertDependency(db(), 100L, "task", 101L, "task");

        // upstream not COMPLETED -> only 100 claimable
        assertEquals(100L, new TaskRepo(db()).claimNext(1L).id());
        assertNull(new TaskRepo(db()).claimNext(1L));

        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET status = 'COMPLETED' WHERE id = 100");
        }
        assertEquals(101L, new TaskRepo(db()).claimNext(1L).id());
    }

    @Test
    void skipsCancelledAndFailedJobs() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "CANCELLED");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");
        assertNull(new TaskRepo(db()).claimNext(1L));
    }

    @Test
    void respectsRetryAfter() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET retry_after = now() + interval '1 hour' WHERE id = 100");
        }
        assertNull(new TaskRepo(db()).claimNext(1L));
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=TaskRepoClaimTest`
Expected: COMPILATION ERROR — `TaskRepo` / `ClaimedTask` do not exist.

- [ ] **Step 4: Implement `ClaimedTask` and `TaskRepo.claimNext`**

`ClaimedTask.java`:

```java
package io.aaiclick.worker.db;

import java.util.List;
import java.util.Map;

/** The subset of a claimed tasks row the shell runner needs. */
public record ClaimedTask(
    long id,
    long jobId,
    String name,
    List<String> command,
    Map<String, String> commandEnv,
    long runEpoch
) {}
```

`TaskRepo.java` (claim only in this task; Task 6 adds the lifecycle methods):

```java
package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/** tasks-table operations: claim (this task) and run lifecycle (Task 6).
 *
 * The claim SQL is PgDbHandler.claim_next_task() + DEPENDENCY_WHERE from
 * aaiclick/orchestration/execution/{pg_handler,db_handler}.py ported
 * verbatim, with two Java-worker capability predicates appended:
 * entry_type = 'shell' AND image_source IS NULL.
 */
public class TaskRepo {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String DEPENDENCY_WHERE = """
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = t.id
            AND d.next_type = 'task'
            AND d.previous_type = 'task'
            AND prev.status != 'COMPLETED'
        )
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = t.id
            AND d.next_type = 'task'
            AND d.previous_type = 'group'
            AND prev.status != 'COMPLETED'
        )
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON d.previous_id = prev.id
            WHERE d.next_id = t.group_id
            AND d.next_type = 'group'
            AND d.previous_type = 'task'
            AND prev.status != 'COMPLETED'
            AND t.group_id IS NOT NULL
        )
        AND NOT EXISTS (
            SELECT 1 FROM dependencies d
            JOIN tasks prev ON prev.group_id = d.previous_id
            WHERE d.next_id = t.group_id
            AND d.next_type = 'group'
            AND d.previous_type = 'group'
            AND prev.status != 'COMPLETED'
            AND t.group_id IS NOT NULL
        )
        """;

    private static final String CLAIM_SQL = """
        WITH claimed_task AS (
            UPDATE tasks
            SET status = 'RUNNING', execution_worker_id = ?, claimed_at = ?
            WHERE id = (
                SELECT t.id FROM tasks t
                JOIN jobs j ON t.job_id = j.id
                WHERE t.status = 'PENDING'
                AND (t.retry_after IS NULL OR t.retry_after <= ?)
                AND j.status NOT IN ('CANCELLED', 'FAILED')
                AND t.entry_type = 'shell'
                AND t.image_source IS NULL
                """ + DEPENDENCY_WHERE + """
                ORDER BY j.started_at ASC NULLS LAST, t.id ASC
                LIMIT 1
                FOR UPDATE OF t SKIP LOCKED
            )
            RETURNING id, job_id, name, command, command_env, run_epoch
        ),
        updated_job AS (
            UPDATE jobs
            SET started_at = COALESCE(started_at, ?),
                status = CASE WHEN started_at IS NULL THEN 'RUNNING' ELSE status END
            WHERE id = (SELECT job_id FROM claimed_task)
            RETURNING id
        )
        SELECT * FROM claimed_task
        """;

    protected final Db db;

    public TaskRepo(Db db) {
        this.db = db;
    }

    public ClaimedTask claimNext(long workerId) throws SQLException {
        try (Connection conn = db.connect(); PreparedStatement stmt = conn.prepareStatement(CLAIM_SQL)) {
            Timestamp now = Timestamp.from(Instant.now());
            stmt.setLong(1, workerId);
            stmt.setTimestamp(2, now);
            stmt.setTimestamp(3, now);
            stmt.setTimestamp(4, now);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new ClaimedTask(
                    rs.getLong("id"),
                    rs.getLong("job_id"),
                    rs.getString("name"),
                    parseJson(rs.getString("command"), new TypeReference<List<String>>() {}),
                    parseJson(rs.getString("command_env"), new TypeReference<Map<String, String>>() {}),
                    rs.getLong("run_epoch"));
            }
        }
    }

    private static <T> T parseJson(String json, TypeReference<T> type) {
        if (json == null) {
            return null;
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Malformed JSON column: " + json, e);
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=TaskRepoClaimTest`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add java/aaiclick-worker
git commit -m "java-worker: task claiming with ported CTE and capability filter"
```

---

### Task 6: Run lifecycle — start, complete, fail, abort check, job completion

**Files:**
- Modify: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/db/TaskRepo.java`
- Test: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/db/TaskRepoLifecycleTest.java`

**Interfaces:**
- Consumes: `ClaimedTask`, `Fixtures` (Task 5).
- Produces, on `TaskRepo`:
  - `void startRun(long taskId, long runId)` — sets `started_at = now()`, appends `runId` to `run_ids` and `"RUNNING"` to `run_statuses` (mirrors the run registration in `runner.py`).
  - `boolean complete(long taskId, long expectedEpoch)` — sets status `COMPLETED`, `completed_at = now()`, last `run_statuses` entry → `COMPLETED`; fenced on `run_epoch`; returns whether a row was updated.
  - `boolean failPendingCleanup(long taskId, long expectedEpoch, String error)` — sets status `PENDING_CLEANUP` + `error`, last `run_statuses` entry → `FAILED`; fenced; returns whether updated. (Mirrors `_set_pending_cleanup()` in `execution_worker.py`.)
  - `boolean isRunAborted(long taskId, long expectedEpoch)` — true when status is `CANCELLED` or `run_epoch != expectedEpoch`; a missing row returns false. (Mirrors `check_run_aborted()` in `claiming.py`.)
  - `void tryCompleteJob(long jobId)` — if no task of the job is in `PENDING/CLAIMED/RUNNING/PENDING_CLEANUP`: job → `FAILED` (error `"One or more tasks failed"`) when any task is `FAILED/UPSTREAM_FAILED`, else `COMPLETED`; sets `completed_at = now()`. No-op otherwise. The `UPSTREAM_FAILED` cascade stays Python-side (`BackgroundWorker`); the Java worker only does the terminal rollup, matching the happy path of `try_complete_job()` in `background/handler.py`.

Implementation note: `run_ids`/`run_statuses` are `JSON` columns — do a read-modify-write inside one transaction with `SELECT ... FOR UPDATE`, editing the arrays via Jackson, exactly like the Python side's locked ORM update.

- [ ] **Step 1: Write the failing test**

```java
package io.aaiclick.worker.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskRepoLifecycleTest extends PgTestBase {

    @BeforeEach
    void clean() throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM dependencies; DELETE FROM tasks; DELETE FROM groups;"
                + " DELETE FROM jobs; DELETE FROM execution_workers;");
        }
    }

    private ClaimedTask claimOne() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"echo\", \"hi\"]");
        return new TaskRepo(db()).claimNext(1L);
    }

    private String scalar(String sql) throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void startRunAppendsRunArrays() throws SQLException {
        ClaimedTask task = claimOne();
        new TaskRepo(db()).startRun(task.id(), 555L);
        assertEquals("[555]", scalar("SELECT run_ids::text FROM tasks WHERE id = 100"));
        assertEquals("[\"RUNNING\"]", scalar("SELECT run_statuses::text FROM tasks WHERE id = 100"));
    }

    @Test
    void completeSetsTerminalStateAndRollsUpJob() throws SQLException {
        ClaimedTask task = claimOne();
        TaskRepo repo = new TaskRepo(db());
        repo.startRun(task.id(), 555L);
        assertTrue(repo.complete(task.id(), task.runEpoch()));
        repo.tryCompleteJob(task.jobId());
        assertEquals("COMPLETED", scalar("SELECT status FROM tasks WHERE id = 100"));
        assertEquals("[\"COMPLETED\"]", scalar("SELECT run_statuses::text FROM tasks WHERE id = 100"));
        assertEquals("COMPLETED", scalar("SELECT status FROM jobs WHERE id = 10"));
    }

    @Test
    void failSetsPendingCleanupAndJobStaysRunning() throws SQLException {
        ClaimedTask task = claimOne();
        TaskRepo repo = new TaskRepo(db());
        repo.startRun(task.id(), 555L);
        assertTrue(repo.failPendingCleanup(task.id(), task.runEpoch(), "exit code 3"));
        repo.tryCompleteJob(task.jobId());
        assertEquals("PENDING_CLEANUP", scalar("SELECT status FROM tasks WHERE id = 100"));
        assertEquals("exit code 3", scalar("SELECT error FROM tasks WHERE id = 100"));
        assertEquals("[\"FAILED\"]", scalar("SELECT run_statuses::text FROM tasks WHERE id = 100"));
        // PENDING_CLEANUP is non-terminal: the BackgroundWorker owns the rest
        assertEquals("RUNNING", scalar("SELECT status FROM jobs WHERE id = 10"));
    }

    @Test
    void epochFencingRejectsStaleWrites() throws SQLException {
        ClaimedTask task = claimOne();
        TaskRepo repo = new TaskRepo(db());
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET run_epoch = run_epoch + 1 WHERE id = 100");  // clear_task
        }
        assertFalse(repo.complete(task.id(), task.runEpoch()));
        assertFalse(repo.failPendingCleanup(task.id(), task.runEpoch(), "late failure"));
        assertEquals("RUNNING", scalar("SELECT status FROM tasks WHERE id = 100"));
    }

    @Test
    void isRunAbortedDetectsCancellationAndEpochBump() throws SQLException {
        ClaimedTask task = claimOne();
        TaskRepo repo = new TaskRepo(db());
        assertFalse(repo.isRunAborted(task.id(), task.runEpoch()));
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET status = 'CANCELLED' WHERE id = 100");
        }
        assertTrue(repo.isRunAborted(task.id(), task.runEpoch()));
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE tasks SET status = 'RUNNING', run_epoch = run_epoch + 1 WHERE id = 100");
        }
        assertTrue(repo.isRunAborted(task.id(), task.runEpoch()));
    }

    @Test
    void tryCompleteJobFailsJobWhenAnyTaskFailed() throws SQLException {
        new WorkerRepo(db()).register(1L, "h", 1);
        Fixtures.insertJob(db(), 10L, "RUNNING");
        Fixtures.insertShellTask(db(), 100L, 10L, "COMPLETED", "[\"echo\"]");
        Fixtures.insertShellTask(db(), 101L, 10L, "FAILED", "[\"echo\"]");
        new TaskRepo(db()).tryCompleteJob(10L);
        assertEquals("FAILED", scalar("SELECT status FROM jobs WHERE id = 10"));
        assertEquals("One or more tasks failed", scalar("SELECT error FROM jobs WHERE id = 10"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=TaskRepoLifecycleTest`
Expected: COMPILATION ERROR — the new methods do not exist.

- [ ] **Step 3: Add the lifecycle methods to `TaskRepo`**

Append to `TaskRepo.java`:

```java
    public void startRun(long taskId, long runId) throws SQLException {
        try (Connection conn = db.connect()) {
            conn.setAutoCommit(false);
            try (PreparedStatement select = conn.prepareStatement(
                     "SELECT run_ids::text, run_statuses::text FROM tasks WHERE id = ? FOR UPDATE")) {
                select.setLong(1, taskId);
                try (ResultSet rs = select.executeQuery()) {
                    if (!rs.next()) {
                        conn.rollback();
                        return;
                    }
                    List<Long> runIds = parseJson(rs.getString(1), new TypeReference<List<Long>>() {});
                    List<String> runStatuses = parseJson(rs.getString(2), new TypeReference<List<String>>() {});
                    runIds = new java.util.ArrayList<>(runIds);
                    runStatuses = new java.util.ArrayList<>(runStatuses);
                    runIds.add(runId);
                    runStatuses.add("RUNNING");
                    try (PreparedStatement update = conn.prepareStatement(
                             "UPDATE tasks SET started_at = ?, run_ids = ?::json, run_statuses = ?::json"
                             + " WHERE id = ?")) {
                        update.setTimestamp(1, Timestamp.from(Instant.now()));
                        update.setString(2, MAPPER.writeValueAsString(runIds));
                        update.setString(3, MAPPER.writeValueAsString(runStatuses));
                        update.setLong(4, taskId);
                        update.executeUpdate();
                    }
                }
            } catch (JsonProcessingException e) {
                conn.rollback();
                throw new IllegalStateException("Failed to serialize run arrays", e);
            }
            conn.commit();
        }
    }

    public boolean complete(long taskId, long expectedEpoch) throws SQLException {
        return finishRun(taskId, expectedEpoch, "COMPLETED", null);
    }

    public boolean failPendingCleanup(long taskId, long expectedEpoch, String error) throws SQLException {
        return finishRun(taskId, expectedEpoch, "PENDING_CLEANUP", error);
    }

    /** Epoch-fenced terminal write; the last run_statuses entry mirrors the
     *  task outcome (COMPLETED, or FAILED for the PENDING_CLEANUP path). */
    private boolean finishRun(long taskId, long expectedEpoch, String status, String error) throws SQLException {
        String runStatus = status.equals("COMPLETED") ? "COMPLETED" : "FAILED";
        try (Connection conn = db.connect()) {
            conn.setAutoCommit(false);
            try (PreparedStatement select = conn.prepareStatement(
                     "SELECT run_statuses::text FROM tasks WHERE id = ? AND run_epoch = ? FOR UPDATE")) {
                select.setLong(1, taskId);
                select.setLong(2, expectedEpoch);
                try (ResultSet rs = select.executeQuery()) {
                    if (!rs.next()) {
                        conn.rollback();
                        return false;
                    }
                    List<String> runStatuses = new java.util.ArrayList<>(
                        parseJson(rs.getString(1), new TypeReference<List<String>>() {}));
                    if (!runStatuses.isEmpty()) {
                        runStatuses.set(runStatuses.size() - 1, runStatus);
                    }
                    try (PreparedStatement update = conn.prepareStatement(
                             "UPDATE tasks SET status = ?, completed_at = ?, error = ?, run_statuses = ?::json"
                             + " WHERE id = ? AND run_epoch = ?")) {
                        update.setString(1, status);
                        update.setTimestamp(2, Timestamp.from(Instant.now()));
                        update.setString(3, error);
                        update.setString(4, MAPPER.writeValueAsString(runStatuses));
                        update.setLong(5, taskId);
                        update.setLong(6, expectedEpoch);
                        boolean updated = update.executeUpdate() > 0;
                        conn.commit();
                        return updated;
                    }
                }
            } catch (JsonProcessingException e) {
                conn.rollback();
                throw new IllegalStateException("Failed to serialize run_statuses", e);
            }
        }
    }

    public boolean isRunAborted(long taskId, long expectedEpoch) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT status, run_epoch FROM tasks WHERE id = ?")) {
            stmt.setLong(1, taskId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return false;
                }
                return "CANCELLED".equals(rs.getString(1)) || rs.getLong(2) != expectedEpoch;
            }
        }
    }

    public void tryCompleteJob(long jobId) throws SQLException {
        try (Connection conn = db.connect();
             PreparedStatement stmt = conn.prepareStatement("""
                 UPDATE jobs SET
                     status = CASE WHEN EXISTS (
                         SELECT 1 FROM tasks WHERE job_id = ?
                         AND status IN ('FAILED', 'UPSTREAM_FAILED')
                     ) THEN 'FAILED' ELSE 'COMPLETED' END,
                     error = CASE WHEN EXISTS (
                         SELECT 1 FROM tasks WHERE job_id = ?
                         AND status IN ('FAILED', 'UPSTREAM_FAILED')
                     ) THEN 'One or more tasks failed' ELSE error END,
                     completed_at = ?
                 WHERE id = ?
                 AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
                 AND NOT EXISTS (
                     SELECT 1 FROM tasks WHERE job_id = ?
                     AND status IN ('PENDING', 'CLAIMED', 'RUNNING', 'PENDING_CLEANUP')
                 )""")) {
            stmt.setLong(1, jobId);
            stmt.setLong(2, jobId);
            stmt.setTimestamp(3, Timestamp.from(Instant.now()));
            stmt.setLong(4, jobId);
            stmt.setLong(5, jobId);
            stmt.executeUpdate();
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=TaskRepoLifecycleTest`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add java/aaiclick-worker
git commit -m "java-worker: run lifecycle with epoch fencing and job rollup"
```

---

### Task 7: Log flusher (task_logs over HTTP)

**Files:**
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/logs/LogLine.java`
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/logs/LogFlusher.java`
- Test: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/logs/LogFlusherTest.java`

**Interfaces:**
- Consumes: `ChClient` (Task 2).
- Produces:
  - `record LogLine(String stream, String level, String text, Instant createdAt)` with factories `LogLine.stdout(String text)` (level `INFO`) and `LogLine.stderr(String text)` (level `WARNING`) stamping `createdAt = Instant.now()` — mirroring the print-capture defaults in `aaiclick/orchestration/logging.py`.
  - `LogFlusher(ChClient ch, long taskId, long jobId, long runId)` — `void add(LogLine line)` (thread-safe), `void flush()` (drains the buffer into one `task_logs` JSONEachRow insert with a strictly increasing `seq` offset), `void close()` (final flush).

`task_logs` columns (from `aaiclick/oplog/migrations/0001_baseline.sql`): `task_id UInt64, job_id UInt64, run_id UInt64, seq UInt64, stream String, level String, line String, created_at DateTime64(3)`. Serialize `created_at` as epoch milliseconds (ClickHouse `DateTime64(3)` accepts numeric input in `JSONEachRow` via `best_effort` parsing — send `"created_at": <epochMillis / 1000.0>` as a decimal number of seconds with 3 fractional digits).

The periodic (2s) scheduling lives in the runner (Task 8) — `LogFlusher` itself is synchronous and testable without timers.

- [ ] **Step 1: Write the failing test**

```java
package io.aaiclick.worker.logs;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.aaiclick.worker.ch.ChClient;
import io.aaiclick.worker.config.WorkerConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class LogFlusherTest {

    @Container
    static final ClickHouseContainer CH = new ClickHouseContainer("clickhouse/clickhouse-server:24.8");

    private ChClient client() {
        WorkerConfig cfg = WorkerConfig.fromEnv(Map.of(
            "AAICLICK_SQL_URL", "postgresql+asyncpg://x:x@unused:5432/x",
            "AAICLICK_CH_URL", "clickhouse://" + CH.getUsername() + ":" + CH.getPassword()
                + "@" + CH.getHost() + ":" + CH.getMappedPort(8123) + "/default"
        ));
        return new ChClient(cfg);
    }

    private void createTaskLogs(ChClient ch) {
        ch.query("""
            CREATE TABLE IF NOT EXISTS task_logs (
                task_id UInt64, job_id UInt64, run_id UInt64, seq UInt64,
                stream String, level String, line String, created_at DateTime64(3)
            ) ENGINE = MergeTree() ORDER BY (task_id, run_id, seq)""");
    }

    @Test
    void flushWritesLinesWithIncreasingSeq() {
        ChClient ch = client();
        createTaskLogs(ch);
        LogFlusher flusher = new LogFlusher(ch, 100L, 10L, 555L);
        flusher.add(LogLine.stdout("first"));
        flusher.add(LogLine.stderr("second"));
        flusher.flush();
        flusher.add(LogLine.stdout("third"));
        flusher.close();

        String rows = ch.query(
            "SELECT seq, stream, level, line FROM task_logs"
            + " WHERE task_id = 100 AND run_id = 555 ORDER BY seq");
        assertEquals("0\tstdout\tINFO\tfirst\n1\tstderr\tWARNING\tsecond\n2\tstdout\tINFO\tthird", rows);
    }

    @Test
    void emptyFlushIsANoop() {
        ChClient ch = client();
        createTaskLogs(ch);
        new LogFlusher(ch, 101L, 10L, 556L).flush();
        assertEquals("0", ch.query("SELECT COUNT(*) FROM task_logs WHERE task_id = 101"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=LogFlusherTest`
Expected: COMPILATION ERROR — `LogFlusher` / `LogLine` do not exist.

- [ ] **Step 3: Implement `LogLine` and `LogFlusher`**

`LogLine.java`:

```java
package io.aaiclick.worker.logs;

import java.time.Instant;

/** One captured output line; level defaults mirror aaiclick print capture:
 *  stdout -> INFO, stderr -> WARNING (ERROR is reserved for real log records). */
public record LogLine(String stream, String level, String text, Instant createdAt) {

    public static LogLine stdout(String text) {
        return new LogLine("stdout", "INFO", text, Instant.now());
    }

    public static LogLine stderr(String text) {
        return new LogLine("stderr", "WARNING", text, Instant.now());
    }
}
```

`LogFlusher.java`:

```java
package io.aaiclick.worker.logs;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.aaiclick.worker.ch.ChClient;

/** Buffers task output and writes it to ClickHouse task_logs in batches.
 *
 * seq is strictly increasing per run_id across flushes (the offset survives
 * the batch), matching _SinkFlusher in aaiclick/orchestration/logging.py.
 * Thread-safe add(); flush()/close() are called from the runner's timer.
 */
public class LogFlusher {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ChClient ch;
    private final long taskId;
    private final long jobId;
    private final long runId;
    private final List<LogLine> buffer = new ArrayList<>();
    private long seqOffset;

    public LogFlusher(ChClient ch, long taskId, long jobId, long runId) {
        this.ch = ch;
        this.taskId = taskId;
        this.jobId = jobId;
        this.runId = runId;
    }

    public synchronized void add(LogLine line) {
        buffer.add(line);
    }

    public void flush() {
        List<LogLine> batch;
        long offset;
        synchronized (this) {
            if (buffer.isEmpty()) {
                return;
            }
            batch = new ArrayList<>(buffer);
            buffer.clear();
            offset = seqOffset;
            seqOffset += batch.size();
        }
        List<ObjectNode> rows = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            LogLine line = batch.get(i);
            ObjectNode row = MAPPER.createObjectNode();
            row.put("task_id", taskId);
            row.put("job_id", jobId);
            row.put("run_id", runId);
            row.put("seq", offset + i);
            row.put("stream", line.stream());
            row.put("level", line.level());
            row.put("line", line.text());
            row.put("created_at", line.createdAt().toEpochMilli() / 1000.0);
            rows.add(row);
        }
        ch.insertJsonEachRow("task_logs", rows);
    }

    public void close() {
        flush();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=LogFlusherTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add java/aaiclick-worker
git commit -m "java-worker: task_logs flusher over ClickHouse HTTP"
```

---

### Task 8: Shell runner — process, env overlay, timeout, abort

**Files:**
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/exec/ShellResult.java`
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/exec/ShellRunner.java`
- Test: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/exec/ShellRunnerTest.java`

**Interfaces:**
- Consumes: `LogLine` (Task 7).
- Produces:
  - `record ShellResult(Outcome outcome, int exitCode)` with `enum Outcome { COMPLETED, FAILED, TIMEOUT, ABORTED }` — `COMPLETED` iff exit code 0.
  - `ShellRunner` with
    `ShellResult run(List<String> command, Map<String,String> commandEnv, Double timeoutSeconds, BooleanSupplier abortCheck, Consumer<LogLine> sink)`.
    Behavior: env = worker process env with `commandEnv` overlaid (subprocess-runner semantics from the spec); stdout/stderr read on two daemon threads into `sink`; polls `abortCheck` every second and destroys the process on true (`ABORTED`); kills on timeout (`TIMEOUT`). Killing uses `destroy()` then `destroyForcibly()` after 5s grace.

- [ ] **Step 1: Write the failing test**

```java
package io.aaiclick.worker.exec;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.aaiclick.worker.logs.LogLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShellRunnerTest {

    private final CopyOnWriteArrayList<LogLine> lines = new CopyOnWriteArrayList<>();

    @Test
    void successCapturesStdout() {
        ShellResult result = new ShellRunner().run(
            List.of("sh", "-c", "echo out; echo err >&2"), null, null, () -> false, lines::add);
        assertEquals(ShellResult.Outcome.COMPLETED, result.outcome());
        assertEquals(0, result.exitCode());
        assertTrue(lines.stream().anyMatch(l -> l.stream().equals("stdout") && l.text().equals("out")));
        assertTrue(lines.stream().anyMatch(l -> l.stream().equals("stderr") && l.text().equals("err")));
    }

    @Test
    void nonZeroExitIsFailed() {
        ShellResult result = new ShellRunner().run(
            List.of("sh", "-c", "exit 3"), null, null, () -> false, lines::add);
        assertEquals(ShellResult.Outcome.FAILED, result.outcome());
        assertEquals(3, result.exitCode());
    }

    @Test
    void commandEnvOverlaysWorkerEnv() {
        ShellResult result = new ShellRunner().run(
            List.of("sh", "-c", "test \"$MY_MARKER\" = overlay && test -n \"$PATH\""),
            Map.of("MY_MARKER", "overlay"), null, () -> false, lines::add);
        assertEquals(ShellResult.Outcome.COMPLETED, result.outcome());
    }

    @Test
    void timeoutKillsProcess() {
        long start = System.nanoTime();
        ShellResult result = new ShellRunner().run(
            List.of("sleep", "30"), null, 1.0, () -> false, lines::add);
        assertEquals(ShellResult.Outcome.TIMEOUT, result.outcome());
        assertTrue((System.nanoTime() - start) / 1_000_000_000.0 < 15);
    }

    @Test
    void abortKillsProcess() {
        long start = System.nanoTime();
        ShellResult result = new ShellRunner().run(
            List.of("sleep", "30"), null, null, () -> true, lines::add);
        assertEquals(ShellResult.Outcome.ABORTED, result.outcome());
        assertTrue((System.nanoTime() - start) / 1_000_000_000.0 < 15);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=ShellRunnerTest`
Expected: COMPILATION ERROR — `ShellRunner` / `ShellResult` do not exist.

- [ ] **Step 3: Implement `ShellResult` and `ShellRunner`**

`ShellResult.java`:

```java
package io.aaiclick.worker.exec;

/** Outcome of one shell task run; COMPLETED iff exit code 0. */
public record ShellResult(Outcome outcome, int exitCode) {

    public enum Outcome { COMPLETED, FAILED, TIMEOUT, ABORTED }
}
```

`ShellRunner.java`:

```java
package io.aaiclick.worker.exec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

import io.aaiclick.worker.logs.LogLine;

/** Runs a shell task argv as a child process.
 *
 * Subprocess-runner semantics: the child inherits the worker's env with
 * command_env overlaid (no isolation boundary on this runner). The abort
 * check is the cancellation monitor — polled every POLL_INTERVAL_MS, it
 * covers cancel_job and clear_task epoch bumps.
 */
public class ShellRunner {

    private static final long POLL_INTERVAL_MS = 1000;
    private static final long KILL_GRACE_MS = 5000;

    public ShellResult run(
            List<String> command,
            Map<String, String> commandEnv,
            Double timeoutSeconds,
            BooleanSupplier abortCheck,
            Consumer<LogLine> sink) {
        ProcessBuilder builder = new ProcessBuilder(command);
        if (commandEnv != null) {
            builder.environment().putAll(commandEnv);
        }
        Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            sink.accept(LogLine.stderr("Failed to launch command: " + e.getMessage()));
            return new ShellResult(ShellResult.Outcome.FAILED, -1);
        }

        Thread stdoutPump = pump(process.getInputStream(), LogLine::stdout, sink);
        Thread stderrPump = pump(process.getErrorStream(), LogLine::stderr, sink);

        Instant deadline = timeoutSeconds == null
            ? null : Instant.now().plus(Duration.ofMillis((long) (timeoutSeconds * 1000)));
        try {
            while (true) {
                if (process.waitFor(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
                    joinPumps(stdoutPump, stderrPump);
                    int exit = process.exitValue();
                    return new ShellResult(
                        exit == 0 ? ShellResult.Outcome.COMPLETED : ShellResult.Outcome.FAILED, exit);
                }
                if (abortCheck.getAsBoolean()) {
                    kill(process);
                    joinPumps(stdoutPump, stderrPump);
                    return new ShellResult(ShellResult.Outcome.ABORTED, -1);
                }
                if (deadline != null && Instant.now().isAfter(deadline)) {
                    kill(process);
                    joinPumps(stdoutPump, stderrPump);
                    return new ShellResult(ShellResult.Outcome.TIMEOUT, -1);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            kill(process);
            return new ShellResult(ShellResult.Outcome.ABORTED, -1);
        }
    }

    private static Thread pump(java.io.InputStream stream, Function<String, LogLine> factory,
            Consumer<LogLine> sink) {
        Thread thread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                     new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sink.accept(factory.apply(line));
                }
            } catch (IOException ignored) {
                // stream closes when the process dies — expected on kill
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void joinPumps(Thread stdoutPump, Thread stderrPump) throws InterruptedException {
        stdoutPump.join(KILL_GRACE_MS);
        stderrPump.join(KILL_GRACE_MS);
    }

    private static void kill(Process process) throws InterruptedException {
        process.destroy();
        if (!process.waitFor(KILL_GRACE_MS, TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            process.waitFor(KILL_GRACE_MS, TimeUnit.MILLISECONDS);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=ShellRunnerTest`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add java/aaiclick-worker
git commit -m "java-worker: shell runner with env overlay, timeout, and abort"
```

---

### Task 9: Worker main loop + graceful shutdown

**Files:**
- Create: `java/aaiclick-worker/src/main/java/io/aaiclick/worker/Worker.java`
- Test: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/WorkerLoopTest.java`

**Interfaces:**
- Consumes: everything above — `WorkerConfig`, `ChClient`, `Db`, `WorkerRepo`, `TaskRepo`, `ClaimedTask`, `ShellRunner`, `ShellResult`, `LogFlusher`, `LogLine`, and test helpers `PgTestBase` / `Fixtures`.
- Produces:
  - `Worker(WorkerConfig cfg)` with `void runLoop(int maxTasks)` — `maxTasks <= 0` means unbounded; `void requestStop()` — finish current task, then exit; `long workerId()`.
  - `public static void main(String[] args)` — reads env via `WorkerConfig.fromEnv(System.getenv())`, parses optional `--max-tasks N`, installs a SIGTERM/SIGINT shutdown hook calling `requestStop()`, runs the loop, exits 0.

Loop per iteration: heartbeat (also detects an externally-set `STOPPING`); claim; if nothing claimed sleep 1s (`POLL_INTERVAL` parity) and continue; else `runId = ch.nextSnowflakeId()`, `startRun`, create `LogFlusher`, run `ShellRunner` with a 2s flush timer thread and `abortCheck = () -> repo.isRunAborted(id, epoch)`; then map the outcome: `COMPLETED` → `complete` + `bumpCompleted`; `FAILED`/`TIMEOUT` → `failPendingCleanup` (+ error `"Command failed with exit code N"` / `"Task timed out after S seconds"`) + `bumpFailed`; `ABORTED` → no status write (cancel/clear owns the row). Always `flusher.close()`. On loop exit: `markStopped`.

- [ ] **Step 1: Write the failing loop test**

Test uses only Postgres (Testcontainers) plus a ClickHouse container for IDs/logs — wire both:

```java
package io.aaiclick.worker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;

import io.aaiclick.worker.ch.ChClient;
import io.aaiclick.worker.config.WorkerConfig;
import io.aaiclick.worker.db.Fixtures;
import io.aaiclick.worker.db.PgTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerLoopTest extends PgTestBase {

    @Container
    static final ClickHouseContainer CH = new ClickHouseContainer("clickhouse/clickhouse-server:24.8");

    private WorkerConfig config() {
        return new WorkerConfig(
            PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(),
            "http://" + CH.getHost() + ":" + CH.getMappedPort(8123),
            CH.getUsername(), CH.getPassword(), "default", null);
    }

    @BeforeEach
    void clean() throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM dependencies; DELETE FROM tasks; DELETE FROM groups;"
                + " DELETE FROM jobs; DELETE FROM execution_workers;");
        }
        new ChClient(config()).query("""
            CREATE TABLE IF NOT EXISTS task_logs (
                task_id UInt64, job_id UInt64, run_id UInt64, seq UInt64,
                stream String, level String, line String, created_at DateTime64(3)
            ) ENGINE = MergeTree() ORDER BY (task_id, run_id, seq)""");
    }

    private String scalar(String sql) throws SQLException {
        try (Connection conn = db().connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void runsShellTaskToCompletionWithLogs() throws Exception {
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"sh\", \"-c\", \"echo from-java\"]");

        Worker worker = new Worker(config());
        worker.runLoop(1);

        assertEquals("COMPLETED", scalar("SELECT status FROM tasks WHERE id = 100"));
        assertEquals("COMPLETED", scalar("SELECT status FROM jobs WHERE id = 10"));
        assertEquals("STOPPED", scalar(
            "SELECT status FROM execution_workers WHERE id = " + worker.workerId()));
        assertEquals("1", scalar(
            "SELECT tasks_completed FROM execution_workers WHERE id = " + worker.workerId()));
        String logged = new ChClient(config()).query(
            "SELECT line FROM task_logs WHERE task_id = 100 AND stream = 'stdout'");
        assertEquals("from-java", logged);
    }

    @Test
    void failingTaskGoesToPendingCleanup() throws Exception {
        Fixtures.insertJob(db(), 10L, "PENDING");
        Fixtures.insertShellTask(db(), 100L, 10L, "PENDING", "[\"sh\", \"-c\", \"exit 7\"]");

        Worker worker = new Worker(config());
        worker.runLoop(1);

        assertEquals("PENDING_CLEANUP", scalar("SELECT status FROM tasks WHERE id = 100"));
        assertTrue(scalar("SELECT error FROM tasks WHERE id = 100").contains("7"));
        assertEquals("RUNNING", scalar("SELECT status FROM jobs WHERE id = 10"));
        assertEquals("1", scalar(
            "SELECT tasks_failed FROM execution_workers WHERE id = " + worker.workerId()));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=WorkerLoopTest`
Expected: COMPILATION ERROR — `Worker` does not exist.

- [ ] **Step 3: Implement `Worker`**

```java
package io.aaiclick.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aaiclick.worker.ch.ChClient;
import io.aaiclick.worker.config.WorkerConfig;
import io.aaiclick.worker.db.ClaimedTask;
import io.aaiclick.worker.db.Db;
import io.aaiclick.worker.db.TaskRepo;
import io.aaiclick.worker.db.WorkerRepo;
import io.aaiclick.worker.exec.ShellResult;
import io.aaiclick.worker.exec.ShellRunner;
import io.aaiclick.worker.logs.LogFlusher;

/** Distributed-mode shell-task worker: claim, execute, report, repeat. */
public class Worker {

    private static final long POLL_INTERVAL_MS = 1000;
    private static final long LOG_FLUSH_INTERVAL_MS = 2000;

    private final WorkerConfig cfg;
    private final ChClient ch;
    private final WorkerRepo workers;
    private final TaskRepo tasks;
    private final ShellRunner runner = new ShellRunner();
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private long workerId;

    public Worker(WorkerConfig cfg) {
        this.cfg = cfg;
        this.ch = new ChClient(cfg);
        Db db = new Db(cfg);
        this.workers = new WorkerRepo(db);
        this.tasks = new TaskRepo(db);
    }

    public long workerId() {
        return workerId;
    }

    public void requestStop() {
        stopRequested.set(true);
    }

    public void runLoop(int maxTasks) throws SQLException, InterruptedException {
        workerId = ch.nextSnowflakeId();
        workers.register(workerId, hostname(), (int) ProcessHandle.current().pid());
        int completed = 0;
        try {
            while (!stopRequested.get() && (maxTasks <= 0 || completed < maxTasks)) {
                String status = workers.heartbeat(workerId);
                if ("STOPPING".equals(status)) {
                    break;
                }
                ClaimedTask task = tasks.claimNext(workerId);
                if (task == null) {
                    Thread.sleep(POLL_INTERVAL_MS);
                    continue;
                }
                runOne(task);
                completed++;
            }
        } finally {
            workers.markStopped(workerId);
        }
    }

    private void runOne(ClaimedTask task) throws SQLException {
        long runId = ch.nextSnowflakeId();
        tasks.startRun(task.id(), runId);
        LogFlusher flusher = new LogFlusher(ch, task.id(), task.jobId(), runId);
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "log-flush-" + task.id());
            t.setDaemon(true);
            return t;
        });
        timer.scheduleAtFixedRate(flusher::flush, LOG_FLUSH_INTERVAL_MS, LOG_FLUSH_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
        try {
            ShellResult result = runner.run(
                task.command(), task.commandEnv(), cfg.taskTimeoutSeconds(),
                () -> {
                    try {
                        return tasks.isRunAborted(task.id(), task.runEpoch());
                    } catch (SQLException e) {
                        return false;  // transient DB error must not kill the child
                    }
                },
                flusher::add);
            switch (result.outcome()) {
                case COMPLETED -> {
                    if (tasks.complete(task.id(), task.runEpoch())) {
                        tasks.tryCompleteJob(task.jobId());
                        workers.bumpCompleted(workerId);
                    }
                }
                case FAILED -> fail(task, "Command failed with exit code " + result.exitCode());
                case TIMEOUT -> fail(task, "Task timed out after " + cfg.taskTimeoutSeconds() + " seconds");
                case ABORTED -> { /* cancel_job / clear_task owns the task row */ }
            }
        } finally {
            timer.shutdown();
            flusher.close();
        }
    }

    private void fail(ClaimedTask task, String error) throws SQLException {
        if (tasks.failPendingCleanup(task.id(), task.runEpoch(), error)) {
            workers.bumpFailed(workerId);
        }
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    public static void main(String[] args) throws Exception {
        int maxTasks = 0;
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--max-tasks")) {
                maxTasks = Integer.parseInt(args[i + 1]);
            }
        }
        Worker worker = new Worker(WorkerConfig.fromEnv(System.getenv()));
        Runtime.getRuntime().addShutdownHook(new Thread(worker::requestStop));
        worker.runLoop(maxTasks);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd java && mvn -q -pl aaiclick-worker test -Dtest=WorkerLoopTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Run the full Java test suite**

Run: `cd java && mvn -q test`
Expected: all tests from Tasks 1–9 PASS.

- [ ] **Step 6: Commit**

```bash
git add java/aaiclick-worker
git commit -m "java-worker: main loop with graceful shutdown"
```

---

### Task 10: CI — Maven job + cross-language integration test

**Files:**
- Modify: `.github/workflows/test.yaml` (add a `java-worker` job)
- Create: `java/aaiclick-worker/src/test/java/io/aaiclick/worker/README.md` — one paragraph documenting that `schema.sql` is a hand-maintained mirror of `aaiclick/orchestration/models.py` and that this CI job is the drift guard.
- Create: `aaiclick/orchestration/execution/test_java_worker_e2e.py`

**Interfaces:**
- Consumes: the shaded jar built by `mvn package` (Task 1's shade plugin); Python fixtures/backends from `aaiclick/conftest.py` (distributed-mode env vars `AAICLICK_SQL_URL` / `AAICLICK_CH_URL`).
- Produces: a pytest module that is skipped unless BOTH distributed backends are configured AND `AAICLICK_JAVA_WORKER_JAR` points at the shaded jar.

The e2e test is the schema-drift guard: it runs the Java worker against a Python-migrated PostgreSQL schema, not the Java test fixture.

- [ ] **Step 1: Write the failing e2e pytest**

`test_java_worker_e2e.py` (follow the `python-testing-style` skill — flat async tests; check existing distributed-marker conventions in `aaiclick/orchestration/execution/test_execution_worker.py` and reuse the same fixtures/skip markers; the sketch below shows intent, align names with the real fixtures):

```python
"""End-to-end: the Java worker claims and completes a shell task.

Requires distributed backends and AAICLICK_JAVA_WORKER_JAR. This is the
schema-drift guard for java/aaiclick-worker/src/test/resources/schema.sql —
here the worker runs against the real Python-migrated schema.
"""

import asyncio
import os
import subprocess

import pytest

from aaiclick.backend import is_local
from aaiclick.orchestration import run_job
from aaiclick.orchestration.jobs import get_job
from aaiclick.orchestration.logging import read_task_logs
from aaiclick.orchestration.jobs.queries import get_tasks_for_job

JAR = os.environ.get("AAICLICK_JAVA_WORKER_JAR")

pytestmark = pytest.mark.skipif(
    is_local() or not JAR,
    reason="requires distributed backends and AAICLICK_JAVA_WORKER_JAR",
)


async def test_java_worker_completes_shell_task():
    job = await run_job(
        "java-e2e",
        entry_type="shell",
        command=["sh", "-c", "echo from-java-worker"],
    )
    proc = subprocess.Popen(
        ["java", "-jar", JAR, "--max-tasks", "1"],
        env=os.environ.copy(),
    )
    try:
        for _ in range(60):
            await asyncio.sleep(1)
            refreshed = await get_job(job.id)
            if refreshed.status == "COMPLETED":
                break
        assert refreshed.status == "COMPLETED"
        tasks = await get_tasks_for_job(job.id)
        entry_task = tasks[0]
        logs = await read_task_logs(entry_task.id, entry_task.run_ids[-1])
        assert any("from-java-worker" in line.text for line in logs)
    finally:
        proc.terminate()
        proc.wait(timeout=30)
```

- [ ] **Step 2: Verify the pytest is collected and skipped locally**

Run: `uv run pytest aaiclick/orchestration/execution/test_java_worker_e2e.py -v`
Expected: SKIPPED (local backends, no jar) — proves collection and imports work.

- [ ] **Step 3: Add the CI job**

In `.github/workflows/test.yaml`, add a job following the existing job layout (read the file first and mirror its checkout/setup steps):

```yaml
  java-worker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "21"
          cache: maven
      - name: Build and test
        run: cd java && mvn -B package
```

Then extend the existing distributed-backend test job (the one that already exports remote `AAICLICK_SQL_URL` / `AAICLICK_CH_URL`): add the setup-java step, `cd java && mvn -B -DskipTests package`, and `AAICLICK_JAVA_WORKER_JAR=$GITHUB_WORKSPACE/java/aaiclick-worker/target/aaiclick-worker-0.0.1-SNAPSHOT.jar` to that job's env so `test_java_worker_e2e.py` stops skipping there.

- [ ] **Step 4: Write the fixture README**

```markdown
schema.sql drift policy
---

`src/test/resources/schema.sql` is a hand-maintained mirror of the tables the
worker touches (`aaiclick/orchestration/models.py`). If a Java test fails
after a Python model change, update the fixture to match. The real guard is
`aaiclick/orchestration/execution/test_java_worker_e2e.py`, which runs the
worker in CI against a Python-migrated schema.
```

- [ ] **Step 5: Push and verify CI**

```bash
git add .github/workflows/test.yaml java/ aaiclick/orchestration/execution/test_java_worker_e2e.py
git commit -m "java-worker: CI build job and cross-language e2e test"
git push -u origin claude/java-worker-distributed-mode-dxi6do
```

Then use the `check-pr` skill to watch the workflows; fix any failures.

---

### Task 11: Documentation

**Files:**
- Modify: `docs/designs/orchestration.md` — add a short "Java worker" subsection under "Distributed runner subtypes" (5–8 lines: shell-only, capability filter, `PENDING_CLEANUP` handoff, `java -jar` invocation, implementation reference `java/aaiclick-worker/src/main/java/io/aaiclick/worker/Worker.java` — see `Worker.runLoop()`).
- Modify: `docs/superpowers/specs/2026-08-08-java-worker-design.md` — per CLAUDE.md, once phase 1 lands the spec's phase-1 sections should be replaced by implementation references; keep phase 2 sections as the design record for future work.

- [ ] **Step 1: Write the docs edits** (apply the `markdown-style` and `shortify` skills)

- [ ] **Step 2: Commit**

```bash
git add docs/
git commit -m "docs: java worker phase 1 references"
```

---

## Self-Review Notes

- **Spec coverage**: config/refusal (Task 1), CH HTTP + snowflake (Task 2), claim + capability filter (Task 5), run lifecycle/fencing/`PENDING_CLEANUP` handoff/job rollup (Task 6), logs (Task 7), env overlay/timeout/cancellation (Task 8), heartbeat/shutdown/loop (Tasks 4, 9), CI + drift guard (Task 10), docs (Task 11). Release wiring (`publish.yaml` java job, fat-jar GitHub Release asset) is deliberately deferred to a follow-up branch — it needs a release tag to test against.
- **Known simplifications vs Python** (accepted for phase 1): `tryCompleteJob` skips the `UPSTREAM_FAILED` cascade (BackgroundWorker also runs it on its poll); heartbeat runs between tasks rather than concurrently during long tasks — acceptable only if task runtimes are typically under `worker_timeout` (90s); if long shell tasks are expected, move `workers.heartbeat()` into the `ShellRunner` poll loop in Task 9 (the abort-check callback already fires every second there).
- The Task 10 pytest sketch must be aligned with real fixture names before use — the plan flags this explicitly in Task 10 Step 1.
