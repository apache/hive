---
name: CVE remediation project status
description: Tracking Hive Docker image CVE remediation - dependency bumps, handler removal, Jetty/Spring upgrades
type: project
---

## Current state (2026-03-26)

Started at 208 vulns, now at ~112 total in the Docker image.

### Completed changes (committed on branch `vuln-fixes-2026-03-26`):
- jackson 2.16.1 → 2.21.1
- zookeeper 3.8.4 → 3.9.5
- commons-lang3 3.17.0 → 3.18.0
- aircompressor 2.0.2 → 2.0.3 (dependency management override)
- lz4-java 1.8.0 → 1.8.1 (dependency management override)
- commons-beanutils 1.9.2 → 1.9.4 (dependency management override)
- commons-vfs2 2.3 → 2.10.0 (dependency management override)
- Docker base image: ubi9 → ubi10 (eclipse-temurin:21.0.10_7-jre-ubi10-minimal)
- Removed druid-handler from packaging (not needed for metastore-only)
- Removed kudu-handler from packaging (not needed for metastore-only)
- Aligned all Spring modules to 5.3.39 via dependency management

### In progress (uncommitted):
- Jetty 9.4.57 → 10.0.26 upgrade
- Two compilation errors remain: `B64Code` removed, `ServerConnector` constructor changed to require `SslContextFactory.Server`

### Next steps:
1. Fix the 2 Jetty 10 compilation errors
2. Build, test, scan, commit
3. Then attempt Jetty 10 → 12 migration (much bigger - requires full jakarta.servlet migration)
4. Spring 6 migration depends on Jetty 12 (Spring Boot 3.x requires jakarta.servlet)

### Architecture notes:
- Hadoop 3.4.x uses javax.servlet + Jetty 9 internally — classpath conflict with jakarta.servlet
- Jetty 12 has EE8 modules that support javax.servlet, but the core server API changed significantly
- Spring is deeply integrated: metastore-server uses Spring JDBC/transactions (80+ files), metastore-rest-catalog is a full Spring Boot app
- Hive's HttpServer.java, PamAuthenticator.java, PamLoginService.java have deep Jetty API usage

### Build/deploy workflow:
- Hadoop built locally at /home/skool/my_code/hadoop-3.4.3
- Tez built locally at /home/skool/my_code/tez (needs repackaging with apache-tez-0.10.5-bin/ prefix)
- Docker cache at hive/packaging/cache/ must be updated manually after rebuilding hadoop/tez
- User only runs Hive Metastore (not HiveServer2, LLAP, etc.)
