---
name: CVE remediation project status
description: Tracking Hive Docker image CVE remediation - dependency bumps, handler removal, Jetty/Spring upgrades
type: project
---

## Current state (2026-03-26)

Started at 208 vulns, now at 112 total. Branch: `vuln-fixes-2026-03-26` (4 commits).

### Completed changes (committed):
1. **7dd72143e6** — Dependency bumps + Docker hardening
   - jackson 2.16.1 → 2.21.1, zookeeper 3.8.4 → 3.9.5, commons-lang3 3.17.0 → 3.18.0
   - aircompressor 2.0.3, lz4-java 1.8.1, commons-beanutils 1.9.4, commons-vfs2 2.10.0 (dep mgmt overrides)
   - Docker base: eclipse-temurin:21.0.10_7-jre-ubi10-minimal
   - Removed druid-handler and kudu-handler from packaging (metastore-only)

2. **954b3faab4** — Aligned all Spring modules to 5.3.39

3. **4e4600ebb2** — Updated scan results

4. **bf59e07836** — Jetty 9.4.57 → 10.0.26
   - B64Code → java.util.Base64
   - SslContextFactory → SslContextFactory.Server
   - Connection.addListener → connector.addBean
   - XmlConfiguration API update
   - Websocket artifact renames, removed jetty-continuation

### Attempted but reverted:
- **Jetty 12 EE8**: Artifact coordinates updated but core API changes in HttpServer.java, PamAuthenticator, PamLoginService, PamConstraint, PamUserIdentity not completed. The Jetty 12 core server API changed fundamentally (Handler.handle signature, Constraint became a record, HandlerCollection removed, etc.)
- **Jetty 12 EE10 + Spring 6**: Additionally hit Thrift 0.16 javax.servlet incompatibility — TServlet extends javax.servlet.http.HttpServlet

### Remaining work for Jetty 12:
Files needing rewrite for Jetty 12 core API (same work for EE8 or EE10):
- `common/src/java/org/apache/hive/http/HttpServer.java` (~1186 lines, deepest Jetty integration)
  - PortHandlerWrapper extends ContextHandlerCollection, overrides handle()
  - Uses HandlerCollection, ContextHandler.Context, LowResourceMonitor, RewriteHandler APIs
  - ServletContextHandler constructor with parent changed
  - setResourceBase → setBaseResourceAsString
  - getHandlers() returns List instead of Handler[]
- `common/src/java/org/apache/hive/http/security/PamAuthenticator.java` — Authenticator interface changed completely
- `common/src/java/org/apache/hive/http/security/PamLoginService.java` — LoginService interface changed
- `common/src/java/org/apache/hive/http/security/PamUserIdentity.java` — DefaultUserIdentity moved
- `common/src/java/org/apache/hive/http/security/PamConstraint.java` — Constraint became a record
- `common/src/java/org/apache/hive/http/security/PamConstraintMapping.java` — ConstraintMapping moved

### Additional work for EE10 + Spring 6:
- javax→jakarta in ~68 Java files (already scripted, trivial)
- Thrift 0.16 → 0.20+ (TServlet uses javax.servlet.http.HttpServlet)
- Spring 5.3→6.2, Spring Boot 2.7→3.4, Spring LDAP 2.4→3.3
- application.yml actuator property path change

### Build/deploy workflow:
- `mvn install -DskipTests` then `mvn clean package -pl packaging -DskipTests -Pdist`
- Copy rebuilt hadoop tar: `cp .../hadoop-3.4.3.tar.gz hive/packaging/cache/`
- Repackage tez: wrap in `apache-tez-0.10.5-bin/` prefix dir, copy to cache
- Docker: `TEZ_URL=file:///...tez... HADOOP_URL=file:///...hadoop... ./packaging/src/docker/build.sh -hadoop 3.4.3`
- Scan: `trivy image apache/hive:4.3.0-SNAPSHOT > snap_1.txt`
- Use `-Drat.skip=true -Dmaven.javadoc.skip=true` to avoid non-code failures

### User context:
- Only uses Hive Metastore (not HiveServer2, LLAP, etc.)
- Hadoop at /home/skool/my_code/hadoop-3.4.3, Tez at /home/skool/my_code/tez
- Jetty 12 is a must (user requirement)
