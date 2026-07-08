<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Apache Hive — Threat Model (v0 draft)

> **Status:** v0 draft produced by the ASF Security team (Michael Scovetta
> rubric, run with Claude Opus) for the Apache Hive PMC to review, correct,
> and own. Every non-trivial claim is provenance-tagged
> *(documented)* / *(maintainer)* / *(inferred)*; the *(inferred)* tags are
> the producer's hypotheses and each has a matching question in §14. Written
> against `master`; revise on a new public-facing surface (a new server
> endpoint, auth mechanism, file format, or execution engine), not on
> internal refactors.
>
> As of the PMC's 2026-07 review, **this in-repo model is the canonical Hive
> security model**. Security information scattered across
> <https://hive.apache.org/> predates it and may be out of date; where they
> disagree, this document wins. *(maintainer — okumin, §14 Q14)*

## §1 — Purpose and consumers

This document describes the **implicit security contract** between Apache
Hive and its downstream operators: what Hive assumes about its environment,
what it upholds given those assumptions, what it leaves to the operator, and
which "syntactically possible" misuses fall outside the intended design. It
serves the **integrator/operator** (which threats they own) and the
**triager** (classifying a scanner/AI/CVE-style finding as valid, out of
model, or disclaimed by design — cite the section).

## §2 — What Hive is

Apache Hive is a **SQL data-warehouse layer over Apache Hadoop** *(documented:
README)*. The in-scope component families:

| Component | Role | Primary surface |
| --- | --- | --- |
| **HiveServer2 (HS2)** | the SQL front door — accepts queries over a Thrift/binary or HTTP transport, authenticates the session, compiles + authorizes + executes | network (the highest-value untrusted boundary) *(inferred — §14 Q1)* |
| **Hive Metastore (HMS)** | a Thrift service holding table/partition/schema metadata + storage locations | network (intra-cluster) *(inferred — §14 Q1)* |
| **Query compiler + execution** | parse → plan → run on Tez / MapReduce / Spark; reads/writes HDFS, HBase, object stores | depends on the configured engine *(documented: README)* |
| **UDF / SerDe / file-format layer** | user-supplied or built-in functions and (de)serializers invoked during execution | in-JVM code execution *(inferred — §14 Q2)* |
| **JDBC/ODBC drivers + Beeline** | client-side connectors | client trust domain |

Hive is **not** a standalone secured appliance: it is a clustered service
deployed behind an operator-controlled perimeter, depending on Hadoop (HDFS,
YARN), a metastore RDBMS, an external authorization provider (typically Apache
Ranger in production deployments, though SQL-standard authorization may also be
used), and a KDC for Kerberos — all treated as trusted dependencies.
*(maintainer — okumin, §14 Q3)*

## §3 — Adversaries in and out of scope

**In scope**:

1. **Untrusted clients at Hive's service boundaries** — a **SQL client**
   submitting statements to HiveServer2, and a **client accessing the Hive
   Metastore through its supported APIs** — attempting to read/modify data
   outside their authorization, or to reach the host through query features.
   *(maintainer — okumin, §14 Q4)*
2. A **network MITM** on the client↔HS2 or HS2↔HMS path, **in scope when TLS
   (or equivalent transport protection) is enabled**; the operator is
   responsible for configuring TLS correctly. *(maintainer — okumin, §14 Q4)*
3. A **direct Metastore (HMS) client.** Some external services (e.g. Apache
   Spark) connect to the Hive Metastore directly, so HMS is expected to enforce
   caller authorization at the **application level** — it is not merely an
   intra-cluster service shielded by a network perimeter. A client reaching HMS
   outside its authorization is in-model. *(maintainer — okumin, §14 Q1.)*

**Out of scope** *(maintainer — okumin, §14 Q5)*:

4. **An operator with `root` / the Hadoop superuser / direct HDFS or metastore-DB
   access.** Anyone who already controls the storage layer or the cluster
   processes is not an adversary Hive defends against → `OUT-OF-MODEL:
   adversary-not-in-scope`.
5. **A trusted authenticated admin** performing an authorized action (creating a
   function, changing config, granting a role). A new path to a privilege the
   principal already holds is `OUT-OF-MODEL: equivalent-harm`.
6. **Bugs in the dependencies Hive orchestrates** — Hadoop/HDFS, YARN, Tez,
   the metastore RDBMS, Ranger, the KDC, the JVM. Report upstream →
   `OUT-OF-MODEL: unsupported-component`.

## §4 — Trust boundaries

- **Client → HiveServer2** is the primary boundary. In a secure, in-model
  deployment, the operator configures authentication (Kerberos / LDAP / PAM /
  custom, etc.) and authorization, and statements are checked by the configured
  authorizer before execution. From HS2's point of view the **SQL
  text, JDBC connection properties, and session-configuration overrides are all
  untrusted**; Hive rejects non-acceptable operations through the authorization
  plugin or through the configured deny lists — `hive.conf.restricted.list`
  (settings an untrusted user may not change), `hive.conf.locked.list`, and
  `hive.conf.hidden.list` (secret values an untrusted user may not read).
  *(maintainer — okumin, §14 Q6)*
- **HiveServer2 → Metastore** and **HS2 → execution engine / HDFS** are
  intra-cluster boundaries. The Metastore is protected at the **application
  level** (it enforces caller authorization), because external services such as
  Spark talk to HMS directly (§3.3); network isolation is defense-in-depth, not
  the primary control *(maintainer — okumin, §14 Q1)*. The HS2 → engine / HDFS
  path is assumed inside an operator-controlled perimeter *(maintainer — okumin, §14 Q3)*.
- **`doAs` impersonation:** when enabled, HS2 executes work as the connected
  end user against HDFS rather than as the Hive service principal; when
  disabled, all access runs as the Hive principal and authorization is fully
  delegated to the SQL-layer authorizer. Because the recommended posture uses
  an authorization plugin (typically Apache Ranger), **`hive.server2.enable.doAs=false`
  is the typical / expected configuration**: HS2 enforces authorization itself
  rather than pushing it down to per-user HDFS permissions. *(maintainer —
  okumin, §14 Q7)*

## §5 — What Hive upholds (given §3/§4 assumptions)

Given valid input and a secure configuration, Hive is expected to uphold the
following. **These properties are configuration-dependent** — they hold when the
operator has configured authentication, authorization, and transport protection
per §6/§8, not by default. *(maintainer — okumin, §14 Q8)*

- **Authentication.** HiveServer2 and the Hive Metastore can require clients to
  authenticate using the configured mechanism (e.g. Kerberos, LDAP), depending
  on the endpoint.
- **Authorization scoping.** Requests are authorized as the authenticated Hive
  session / metastore user through the configured authorization manager
  (typically Apache Ranger). Decisions are scoped to Hive objects and operations
  — databases, tables, partitions, columns, functions, and the relevant metadata
  operations.
- **Metastore authorization.** When the Hive Metastore is directly exposed to
  clients, metastore-side authorization can be enforced through
  `HiveMetaStoreAuthorizer` as a pre-event listener, and metadata read/list
  results can be filtered server-side via the metastore filter hook.
- **Transport protection.** HiveServer2, the Hive Metastore, and the HS2 Web UI
  can protect traffic confidentiality and integrity when TLS/SSL is enabled and
  correctly configured.
- **Configuration protection.** Sensitive or security-critical configuration
  values can be restricted, locked, or hidden (§4/§8 deny lists) so untrusted
  users cannot change security posture at runtime or read secret values.
- **Credential handling.** Secrets can be externalized through Hadoop credential
  providers rather than stored in cleartext configuration files.
- **Memory safety on well-formed input** to the extent the JVM provides it;
  Hive is Java, so classic memory-corruption is out of the language model.

Hive does **not** by itself guarantee authorization for a caller who bypasses
Hive and accesses the underlying storage or metastore database directly; those
paths must be protected separately by HDFS / object-store / IAM / database
controls. *(maintainer — okumin)*

## §6 — What Hive leaves to the operator

*(maintainer — okumin, §14 Q9)*

- **Transport security (TLS)** on the HS2, Metastore, and HS2 Web UI endpoints,
  and the KDC / LDAP server's own security.
- **Choosing and configuring an authorization model.** **Apache Ranger is the
  primary authorization plugin** for Hive; storage-based and SQL-standard
  authorization give materially different guarantees. The operator owns the
  choice (and whether "no authorization configured" is a supported production
  mode — it is not, for an internet- or multi-tenant-exposed deployment).
- **Network isolation** of the metastore RDBMS and the execution cluster from
  untrusted networks.
- **Vetting UDFs / SerDes / aux JARs.** Adding a function or SerDe is adding
  code to the execution JVM (see §7).

## §7 — Properties Hive does *not* uphold (by design)

*(maintainer — okumin, §14 Q10: confirmed)*

- **A sandbox around UDFs, SerDes, custom InputFormats, or `TRANSFORM`/script
  operators.** Code a principal is authorized to register or invoke runs with
  the privileges of the execution process; this is a feature, not a
  containment boundary. `BY-DESIGN: property-disclaimed`. The model assumes
  authentication and authorization are configured; against that baseline
  *(maintainer — okumin)*:
    - **Built-in UDFs** are generally safe, but Hive ships a few that allow
      arbitrary code execution (`reflect`, `reflect2`, `java_method`,
      `in_file`). A Hive administrator must block these via
      `hive.server2.builtin.udf.blacklist` (directly or through the
      authorization plugin); major plugins such as the Ranger authorizer
      configure this blacklist properly.
    - **Custom UDFs** — the administrator must restrict, via access policies
      (e.g. Ranger), who may add UDF jars or register UDFs. Those trusted users
      are responsible for safe UDFs; Hive cannot guarantee safety when a
      trusted user adds a compromised UDF.
    - **SerDe / InputFormat / OutputFormat** — only administrators can install
      the jars; they are responsible for deploying safe implementations, and
      Hive trusts them. Hive cannot guarantee safety against a compromised one
      an admin installs.
    - **`TRANSFORM`** — in a secure deployment it must be prohibited. Major
      authorization plugins add
      `org.apache.hadoop.hive.ql.security.authorization.plugin.DisallowTransformHook`
      to `hive.exec.pre.hooks`; administrators are responsible for using such a
      plugin or configuring the hook themselves.
- **Protection against an operator who controls the underlying storage,
  metastore DB, or cluster processes** (see §3 item 4).
- **Resource fairness / DoS protection as a hard guarantee.** Hive accepts
  arbitrary HiveQL, so bounding the impact of a pathological query is an
  operator responsibility, not a Hive bug: operators use HiveServer2 limits
  (e.g. `hive.query.max.length`) and YARN resource pools, and — where stronger
  isolation than HS2 + YARN can provide is required — separate HS2 instances or
  separate Hadoop/YARN clusters. *(maintainer — okumin, §14 Q11)*

## §8 — Key configuration levers (load-bearing)

The security posture is configuration-dependent; the levers below are what an
operator sets to reach the §5 properties. Organized by component per the PMC's
structure preference (§14 Q15). *(maintainer — okumin, §14 Q12)*

### §8.1 — HiveServer2

- **Authentication:** `hive.server2.authentication` and the more-specific
  `hive.server2.authentication.*` parameters must be configured properly.
- **Authorization:** `hive.security.authorization.enabled=true` and
  `hive.security.authorization.manager` (typically Apache Ranger). With Ranger,
  `hive.security.authenticator.manager` should be
  `org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator` to authorize
  as the HS2 session user, and `hive.server2.enable.doAs` is typically `false`.
- **TLS:** `hive.server2.use.SSL=true` plus the more-specific TLS parameters.
- **Config protection:** a Hive administrator should protect security-related
  settings via `hive.conf.restricted.list`, `hive.conf.locked.list`, and
  `hive.conf.hidden.list` so untrusted users cannot update sensitive settings or
  view secret values.
- **Code-execution levers:** built-in UDF blacklist
  `hive.server2.builtin.udf.blacklist` blocks the code-exec built-ins
  (`reflect`, `reflect2`, `java_method`, `in_file`); `DisallowTransformHook` in
  `hive.exec.pre.hooks` prohibits `TRANSFORM`/script operators. Major
  authorization plugins (e.g. Ranger) configure both.

Reference: <https://hive.apache.org/docs/latest/admin/setting-up-hiveserver2/#authenticationsecurity-configuration>

### §8.2 — HiveServer2 Web UI

If the UI port (default `10002`) is exposed to untrusted users:

- **Authentication:** at least one of `hive.server2.webui.use.spnego=true`,
  `hive.server2.webui.use.pam=true`, or `hive.server2.webui.auth.method=LDAP`
  (plus the more-specific parameters).
- **TLS:** `hive.server2.webui.use.ssl=true` plus the more-specific TLS
  parameters.

### §8.3 — Hive Metastore

If a Hive administrator exposes the Metastore to untrusted users or systems
(e.g. Spark, Flink):

- **Authentication:** `metastore.authentication` (e.g. `KERBEROS` or `LDAP`)
  plus the more-specific parameters (`metastore.sasl.enabled`,
  `metastore.kerberos.*` / `metastore.authentication.*`).
- **Authorization:** `hive.security.authorization.manager` set to
  `RangerHiveAuthorizerFactory` (typical), `metastore.pre.event.listeners`
  including
  `org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer`,
  and `metastore.server.filter.enabled=true` with `metastore.filter.hook` set to
  the same `HiveMetaStoreAuthorizer` for server-side result filtering.
- **TLS:** `metastore.use.SSL` plus the more-specific TLS parameters.

### §8.4 — Credentials (all components)

All credentials / secrets should be managed through the Hadoop
`CredentialProviderAPI`, configured via `hadoop.security.credential.provider.path`
(<https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html>).
Hive does **not** guarantee that the content of `hive-site.xml`,
`metastore-site.xml`, environment variables, or system properties is never
exposed — secrets placed there are the administrator's risk (see §11a).
*(maintainer — okumin)*

## §11a — Known non-findings (seed for scanner/AI triage)

These are the dispositions the PMC has recorded; confirm and extend
*(maintainer — okumin, §14 Q13)*:

- Hive defaults are not a complete secure-production baseline; some defaults
  prioritize ease of adoption, compatibility, or trusted/local deployments.
  Operators are responsible for enabling and correctly configuring authentication,
  authorization, transport security, network isolation, and related controls for
  internet-exposed or multi-tenant production deployments. `OUT-OF-MODEL:
  operator responsibility`
- **"Secrets are exposed in `hive-site.xml` / `metastore-site.xml` / environment
  variables / system properties."** By design not a Hive defect — a Hive
  administrator who stores secrets in those places has chosen the wrong
  mechanism. Secrets should be managed through the Hadoop `CredentialProviderAPI`
  (§8.4). Hive does not guarantee the content of those config surfaces is never
  exposed. `OUT-OF-MODEL: operator responsibility`. *(maintainer — okumin.)*
- **"A UDF / `TRANSFORM` script / custom SerDe can run arbitrary code."**
  By design — registering or invoking code is an authorized operation, not a
  sandbox escape. `BY-DESIGN`.
- **"A built-in UDF (`reflect`/`reflect2`/`java_method`/`in_file`) enables code
  execution."** By design — these are blocked by the administrator via
  `hive.server2.builtin.udf.blacklist` (Ranger does this). Reachable only when
  the operator has not configured the blacklist → `OUT-OF-MODEL:
  operator responsibility`, unless the report shows a
  *bypass* of a configured blacklist. *(maintainer — okumin.)*
- **Dependency-tail CVEs** (Hadoop, Log4j, a transitive JAR) surfaced by an
  SCA scanner against Hive's build — triage upstream unless Hive's own code
  reaches the vulnerable path with untrusted input.

## §13 — Triage dispositions

A finding is **VALID** only when all hold: the violated property is one Hive
claims (§5), the attacker is in scope (§3), and the affected code is on an
in-model surface (§2/§4) reached by untrusted input. Otherwise route to one
of: `OUT-OF-MODEL: adversary-not-in-scope` · `OUT-OF-MODEL: equivalent-harm` ·
`OUT-OF-MODEL: unsupported-component` · `OUT-OF-MODEL: operator responsibility` ·
`BY-DESIGN: property-disclaimed`.

## §14 — Open questions for the Hive PMC

Grouped in waves; answer inline (a few at a time is fine). Each promotes an
*(inferred)* tag to *(maintainer)* once confirmed.

**Wave 1 — scope & intended use**
1. *(Answered — okumin: direct Metastore access **is** in scope; HMS enforces
   caller authorization at the application level, since external services like
   Spark talk to it directly. Folded into §3.3 / §4 / §11a.)* ~~Is the in-scope
   surface "HiveServer2 + the artifacts it compiles/executes", with the
   Metastore treated as intra-cluster trusted? Or is direct Metastore access in
   scope?~~
2. *(Answered — okumin: yes, in scope as code-execution-by-design, with detail
   on built-in UDF blacklist, custom UDF/SerDe admin trust, and `TRANSFORM`
   disable via `DisallowTransformHook`. Folded into §7 / §8 / §11a.)* ~~Are
   UDFs / SerDes / custom InputFormats / `TRANSFORM` scripts in scope as
   code-execution-by-design (not a sandbox), per §7?~~
3. *(Answered — okumin: confirmed. Clustered, operator-controlled perimeter;
   Hadoop, the metastore RDBMS, the authorization provider (typically Ranger in
   production, SQL-standard also supported), and the KDC are trusted
   dependencies. Folded into §2 / §4.)* ~~Confirm the assumed deployment:
   clustered, behind an operator-controlled perimeter, with Hadoop + a metastore
   RDBMS + (Ranger or SQL-std auth) + KDC as trusted dependencies.~~
4. *(Answered — okumin: the primary in-scope adversaries are untrusted clients
   at Hive's service boundaries — SQL clients submitting to HS2 and clients
   reaching the Metastore through supported APIs; a network MITM is in scope
   when TLS / equivalent transport protection is enabled (operator-configured).
   Folded into §3.1 / §3.2.)* ~~Is the in-scope adversary "a SQL client at the
   HS2 boundary" (+ a network MITM where TLS is off)? Anything to add?~~
5. *(Answered — okumin: confirmed. Operators with direct storage / metastore-DB
   / cluster-process access are trusted and out of scope; authorized actions by
   trusted admins are out of model. Folded into §3.4 / §3.5.)* ~~Confirm
   operators with storage/metastore-DB/cluster-process access, and trusted
   admins doing authorized actions, are out of model.~~

**Wave 2 — trust boundaries & auth**
6. *(Answered — okumin: from HS2's point of view SQL text, JDBC connection
   properties, and session config are untrusted; Hive rejects non-acceptable
   operations via the authz plugin or the deny lists
   (`hive.conf.restricted.list` / `hive.conf.locked.list` /
   `hive.conf.hidden.list`). Folded into §4.)* ~~At the client→HS2 boundary, are
   SQL text, JDBC connection properties, and session-config overrides all
   treated as untrusted (subject to the conf whitelist)?~~
7. *(Answered — okumin: because the recommended posture uses an authorization
   plugin (Apache Ranger), `hive.server2.enable.doAs=false` is the typical /
   expected configuration; HS2 enforces authorization itself. Folded into §4 /
   §8.1.)* ~~Which `doAs` posture is the supported/recommended one, and how does
   it change the authorization story?~~
8. *(Answered — okumin: authentication, authorization scoping, metastore
   authorization, transport protection, configuration protection, credential
   handling — all configuration-dependent; no guarantee for callers who bypass
   Hive and hit storage / the metastore DB directly. Folded into §5.)* ~~What
   properties does Hive claim to uphold given valid input (auth, authz scoping,
   others)?~~

**Wave 3 — disclaimed properties & defaults**
9. *(Answered — okumin: Apache Ranger is the primary authorization plugin; the
   §6 operator-owned list stands. Folded into §6.)* ~~Confirm the operator-owned
   list in §6 (TLS, authz-model choice, network isolation, UDF vetting).~~
10. *(Answered — okumin: confirmed ("OK"). §7 by-design non-guarantees stand.)*
    ~~Confirm the by-design non-guarantees in §7.~~
11. *(Answered — okumin: the operator's job, not a Hive bug — Hive accepts
    arbitrary HiveQL; operators bound it with HS2 limits (e.g.
    `hive.query.max.length`) + YARN pools, and separate HS2 instances / clusters
    for stronger isolation. Folded into §7.)* ~~Is super-linear resource use / a
    hang on a pathological query a bug, or is bounding it the operator's job
    (YARN queues / HS2 limits)?~~
12. *(Answered — okumin: supplied the real parameter names + secure-config
    recipes for HiveServer2, the HS2 Web UI, and the Hive Metastore, plus the
    Hadoop `CredentialProviderAPI`. Folded into §8.1–§8.4.)* ~~Confirm the real
    names + shipped defaults of the §8 levers.~~
13. *(Answered — okumin: secret exposure when an admin stores secrets in
    `hive-site.xml` / `metastore-site.xml` / env vars / system properties
    instead of using the Hadoop `CredentialProviderAPI`. Folded into §11a.)*
    ~~What do scanners/fuzzers/researchers most often report that you consider a
    non-finding?~~

**Wave 4 — meta**
14. *(Answered — okumin: as of today this in-repo model is the canonical page.
    Some security information is scattered across <https://hive.apache.org/> and
    may not be up to date; the in-repo model supersedes it. Reflected in §1.)*
    ~~Confirm this in-repo model is canonical (vs the cwiki security pages).~~
15. *(Answered — okumin: keep **one file**, split into clearly-labelled
    HiveServer2 and Hive Metastore sections rather than two separate files.
    Applied: §8 is now organized into §8.1 HiveServer2 / §8.2 HS2 Web UI / §8.3
    Hive Metastore / §8.4 Credentials, and the component boundaries are labelled
    in §2/§3/§4. If deeper per-component scope/adversary subsections would help,
    say so and we'll extend — the structure is the PMC's call.)* ~~Should HMS
    get its own separate `THREAT_MODEL.md`?~~
