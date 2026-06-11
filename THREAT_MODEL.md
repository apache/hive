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
that depends on Hadoop (HDFS, YARN), a metastore RDBMS, and typically an
external authorization service (Apache Ranger or SQL-standard authorization)
and a KDC for Kerberos *(inferred — §14 Q3)*.

## §3 — Adversaries in and out of scope

**In scope** *(inferred — §14 Q4)*:

1. A **SQL client** connecting to HiveServer2 with valid or attempted-invalid
   credentials, trying to read/modify data outside their authorization, or to
   reach the host through query features.
2. A **network adversary** between client and HS2 / between HS2 and HMS, where
   transport security is not configured.

**Out of scope** *(inferred — §14 Q5)*:

3. **An operator with `root` / the Hadoop superuser / direct HDFS or metastore-DB
   access.** Anyone who already controls the storage layer or the cluster
   processes is not an adversary Hive defends against → `OUT-OF-MODEL:
   adversary-not-in-scope`.
4. **A trusted authenticated admin** performing an authorized action (creating a
   function, changing config, granting a role). A new path to a privilege the
   principal already holds is `OUT-OF-MODEL: equivalent-harm`.
5. **Bugs in the dependencies Hive orchestrates** — Hadoop/HDFS, YARN, Tez,
   the metastore RDBMS, Ranger, the KDC, the JVM. Report upstream →
   `OUT-OF-MODEL: unsupported-component`.

## §4 — Trust boundaries

- **Client → HiveServer2** is the primary boundary. The session is
  authenticated (Kerberos / LDAP / PAM / custom / none) and every statement is
  authorization-checked before execution *(inferred — §14 Q6)*. Whether the SQL
  text, JDBC connection properties, and session-configuration overrides are
  treated as untrusted at this boundary is the load-bearing question for
  triage.
- **HiveServer2 → Metastore** and **HS2 → execution engine / HDFS** are
  intra-cluster boundaries assumed to run inside an operator-controlled,
  network-isolated perimeter *(inferred — §14 Q3)*.
- **`doAs` impersonation:** when enabled, HS2 executes work as the connected
  end user against HDFS rather than as the Hive service principal; when
  disabled, all access runs as the Hive principal and authorization is fully
  delegated to the SQL-layer authorizer *(inferred — §14 Q7)*. The two modes
  have materially different blast radii and which is "the" supported posture
  is a §14 question.

## §5 — What Hive upholds (given §3/§4 assumptions)

*(all inferred — §14 Q8)*

- **Authentication** of the HS2 session via the configured mechanism before any
  statement runs.
- **Authorization** of each statement against the configured model (Ranger /
  SQL-standard / storage-based), scoped to the principal's granted privileges
  on the named objects.
- **Memory safety on well-formed input** to the extent the JVM provides it;
  Hive is Java, so classic memory-corruption is out of the language model.

## §6 — What Hive leaves to the operator

*(inferred — §14 Q9)*

- **Transport security (TLS)** on the HS2 and Metastore endpoints, and the KDC
  / LDAP server's own security.
- **Choosing and configuring an authorization model.** Storage-based and
  SQL-standard and Ranger give materially different guarantees; the default
  posture (and whether "no authorization configured" is a supported production
  mode) is the operator's call.
- **Network isolation** of the Metastore, the metastore RDBMS, and the
  execution cluster from untrusted networks.
- **Vetting UDFs / SerDes / aux JARs.** Adding a function or SerDe is adding
  code to the execution JVM (see §9).

## §7 — Properties Hive does *not* uphold (by design)

*(inferred — §14 Q10)*

- **A sandbox around UDFs, SerDes, custom InputFormats, or `TRANSFORM`/script
  operators.** Code a principal is authorized to register or invoke runs with
  the privileges of the execution process; this is a feature, not a
  containment boundary. `BY-DESIGN: property-disclaimed`.
- **Protection against an operator who controls the underlying storage,
  metastore DB, or cluster processes** (see §3 item 3).
- **Resource fairness / DoS protection as a hard guarantee.** A sufficiently
  expensive query can exhaust cluster resources; per-pool/queue limits
  (YARN, HS2 query limits) are the operator's lever, not an engine invariant
  *(inferred — §14 Q11)*.

## §8 — Key configuration levers (load-bearing)

*(all inferred — §14 Q12; confirm names + defaults)*

| Lever | Why it matters |
| --- | --- |
| Authentication mode (`hive.server2.authentication`) | `NONE` vs `KERBEROS`/`LDAP` decides whether the front door is open. |
| Authorization model (Ranger / SQL-std / storage-based / none) | decides whether statements are access-controlled at all. |
| `doAs` (`hive.server2.enable.doAs`) | decides whether HDFS access runs as the end user or the Hive principal. |
| TLS on HS2 / HMS transports | decides whether sessions + metadata are on the wire in clear. |
| UDF/SerDe allow-listing, `hive.security.authorization.sqlstd.confwhitelist` | decides which session config + functions an untrusted client may set/use. |

## §11a — Known non-findings (seed for scanner/AI triage)

These are the dispositions the PMC most likely wants pre-recorded; confirm and
extend *(inferred — §14 Q13)*:

- **"A UDF / `TRANSFORM` script / custom SerDe can run arbitrary code."**
  By design — registering or invoking code is an authorized operation, not a
  sandbox escape. `BY-DESIGN`.
- **"HiveServer2 with `authentication=NONE` accepts anyone."** That is a
  non-default / operator-chosen insecure configuration, not a Hive defect.
  `OUT-OF-MODEL: non-default-build` (confirm the shipped default).
- **"The Metastore Thrift port has no authorization."** In-model only if the
  PMC asserts HMS is meant to enforce caller authorization; if HMS is an
  intra-cluster trusted service behind the perimeter, reports against direct
  HMS access are `OUT-OF-MODEL: adversary-not-in-scope`. (§14 Q1/Q3.)
- **Dependency-tail CVEs** (Hadoop, Log4j, a transitive JAR) surfaced by an
  SCA scanner against Hive's build — triage upstream unless Hive's own code
  reaches the vulnerable path with untrusted input.

## §13 — Triage dispositions

A finding is **VALID** only when all hold: the violated property is one Hive
claims (§5), the attacker is in scope (§3), and the affected code is on an
in-model surface (§2/§4) reached by untrusted input. Otherwise route to one
of: `OUT-OF-MODEL: adversary-not-in-scope` · `OUT-OF-MODEL: equivalent-harm` ·
`OUT-OF-MODEL: unsupported-component` · `OUT-OF-MODEL: non-default-build` ·
`BY-DESIGN: property-disclaimed`.

## §14 — Open questions for the Hive PMC

Grouped in waves; answer inline (a few at a time is fine). Each promotes an
*(inferred)* tag to *(maintainer)* once confirmed.

**Wave 1 — scope & intended use**
1. Is the in-scope surface "HiveServer2 (the SQL front door) + the artifacts it
   compiles/executes", with the Metastore treated as an intra-cluster trusted
   service? Or is direct Metastore access in scope?
2. Are UDFs / SerDes / custom InputFormats / `TRANSFORM` scripts in scope as
   *code-execution-by-design* (not a sandbox), per §7?
3. Confirm the assumed deployment: clustered, behind an operator-controlled
   perimeter, with Hadoop + a metastore RDBMS + (Ranger or SQL-std auth) + KDC
   as trusted dependencies.
4. Is the in-scope adversary "a SQL client at the HS2 boundary" (+ a network
   MITM where TLS is off)? Anything to add?
5. Confirm operators with storage/metastore-DB/cluster-process access, and
   trusted admins doing authorized actions, are out of model.

**Wave 2 — trust boundaries & auth**
6. At the client→HS2 boundary, are SQL text, JDBC connection properties, and
   session-config overrides all treated as untrusted (subject to the conf
   whitelist)?
7. Which `doAs` posture is the supported/recommended one, and how does it
   change the authorization story?
8. What properties does Hive claim to uphold given valid input (auth, authz
   scoping, others)?

**Wave 3 — disclaimed properties & defaults**
9. Confirm the operator-owned list in §6 (TLS, authz-model choice, network
   isolation, UDF vetting). Anything mis-assigned?
10. Confirm the by-design non-guarantees in §7.
11. Is super-linear resource use / a hang on a pathological query a bug, or is
    bounding it the operator's job (YARN queues / HS2 limits)?
12. Confirm the real names + shipped defaults of the §8 levers (especially
    `hive.server2.authentication` and the default authorization model).
13. What do scanners/fuzzers/researchers most often report that you consider a
    non-finding? (Feeds §11a.)

**Wave 4 — meta**
14. Hive has no in-repo `SECURITY.md`/`THREAT_MODEL.md` today; this PR adds
    them and wires `AGENTS.md → SECURITY.md → THREAT_MODEL.md`. Confirm this
    in-repo model is canonical (vs the cwiki security pages), how it should
    reference those pages, and who owns revisions.
