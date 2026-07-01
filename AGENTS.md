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

# AGENTS.md

## Security

You are helping a security researcher find and report vulnerabilities in
Apache Hive. Before drafting any report or reaching any conclusion, complete
these steps.

### Step 1 — Read the threat model
Read **[THREAT_MODEL.md](THREAT_MODEL.md)**: the trust boundaries (the
HiveServer2 SQL front door, the Metastore, the query/UDF execution layer), the
adversaries in and out of scope, and what Hive upholds versus what it leaves to
the operator.

### Step 2 — Read the security policy
Read **[SECURITY.md](SECURITY.md)** for how to report.

### Key scoping facts (see THREAT_MODEL.md)
- The **HiveServer2** SQL front door is the primary untrusted boundary; the
  Metastore and execution cluster are assumed to run inside an
  operator-controlled perimeter.
- **UDFs, SerDes, custom InputFormats, and `TRANSFORM` scripts are
  code-execution by design**, not a sandbox — running authorized code is a
  feature, not a vulnerability.
- Transport security (TLS), the choice of authorization model (Ranger /
  SQL-standard / storage-based), and network isolation are **operator**
  responsibilities, not engine invariants.
- Hive does **not** defend against an operator with `root`, the Hadoop
  superuser, or direct HDFS / metastore-DB access.

### Step 3 — Route the finding
Route the finding to exactly one disposition in **THREAT_MODEL.md §13**
(VALID, or one of the `OUT-OF-MODEL` / `BY-DESIGN` dispositions) and cite the
section that justifies the call. This model is **v0** — open questions for the
PMC are in §14.
