/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.model;

/**
 * Normalised identity-value row for an erasure run: one row per
 * (run, identity value), so the subject-keyed inverse audit
 * ({@code AUDIT BY IDENTITY VALUES}) is an indexed exact-match lookup on
 * {@code identityValue} rather than a substring scan of the parent run's
 * serialised {@code identityValues} CLOB.
 *
 * <p>The parent is referenced by its raw {@code runId} (the
 * {@link MErasureRunAudit} datastore id), not as a JDO relationship: a
 * relationship would make DataNucleus create a foreign key, and under
 * lazy schema management (a fresh autoCreate metastore) it does so during
 * the first {@code recordErasureRun}, inside the transaction that has just
 * inserted the parent row, deadlocking the {@code ALTER TABLE ... ADD FK}
 * against the parent's own lock. The parent is always present (the audit
 * table is append-only), so a plain id column is sufficient; the production
 * DDL keeps an explicit foreign key, which schematool applies up front.
 */
public class MErasureRunAuditIdentity {

  private long runId;
  private String identityValue;

  public MErasureRunAuditIdentity() {
  }

  public MErasureRunAuditIdentity(long runId, String identityValue) {
    this.runId = runId;
    this.identityValue = identityValue;
  }

  public long getRunId() {
    return runId;
  }

  public void setRunId(long runId) {
    this.runId = runId;
  }

  public String getIdentityValue() {
    return identityValue;
  }

  public void setIdentityValue(String identityValue) {
    this.identityValue = identityValue;
  }
}
