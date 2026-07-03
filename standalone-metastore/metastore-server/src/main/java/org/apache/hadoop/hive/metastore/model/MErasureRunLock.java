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
 * Per-table run-lock for {@code ERASE FROM TABLE} commands. Only one
 * lock with status
 * {@code RUNNING} may exist per {@code tblId}; the lock is released
 * either by the run itself on clean completion or by an explicit
 * {@code RELEASE ERASURE LOCK ON TABLE t} command.
 *
 * <p>The model is deliberately manual-recovery rather than
 * lease/heartbeat: a crashed run leaves the lock {@code RUNNING}, and
 * an operator inspects via {@code SHOW ERASURE LOCK} and releases via
 * {@code RELEASE ERASURE LOCK} after confirming the previous run is
 * dead. This fits the assumed workload of a small table set with
 * scheduled batch ERASE runs rather than high-frequency ad-hoc
 * invocations.
 */
public class MErasureRunLock {

  // Composite identity column: one row per tblId. The acquire path
  // refuses if a row with status RUNNING already exists for the table.
  private long tblId;

  // The runId of the holder. Matches MErasureRunAudit.runId so a
  // compliance query can join the two tables and see "lock held by
  // run X, which started at T0 and was last seen at T1 with status Y".
  private long runId;

  // Principal that acquired the lock (the operator or scheduler).
  private String principal;

  // Wall-clock acquisition time.
  private long startedTs;

  // Wall-clock completion time, set when the run releases the lock
  // cleanly. Null for in-progress and abandoned runs.
  private Long completedTs;

  // Principal that explicitly released the lock via RELEASE ERASURE
  // LOCK. Null when status is RUNNING or COMPLETED.
  private String releasedBy;

  // Wall-clock time of explicit release.
  private Long releasedTs;

  // Free-text justification supplied to RELEASE ERASURE LOCK ... WITH
  // REASON 'text'. Recorded verbatim for the audit trail.
  private String releaseReason;

  // Lock status. One of:
  //   RUNNING        - acquired, not yet released
  //   COMPLETED      - released by the run on clean exit
  //   RELEASED       - released manually after the .anon.tmp safety check passed
  //   FORCE_RELEASED - released manually with FORCE, bypassing the safety check
  private String status;

  public MErasureRunLock() {
  }

  public MErasureRunLock(long tblId, long runId, String principal,
                         long startedTs, String status) {
    this.tblId = tblId;
    this.runId = runId;
    this.principal = principal;
    this.startedTs = startedTs;
    this.status = status;
  }

  public long getTblId() { return tblId; }
  public void setTblId(long tblId) { this.tblId = tblId; }

  public long getRunId() { return runId; }
  public void setRunId(long runId) { this.runId = runId; }

  public String getPrincipal() { return principal; }
  public void setPrincipal(String principal) { this.principal = principal; }

  public long getStartedTs() { return startedTs; }
  public void setStartedTs(long startedTs) { this.startedTs = startedTs; }

  public Long getCompletedTs() { return completedTs; }
  public void setCompletedTs(Long completedTs) { this.completedTs = completedTs; }

  public String getReleasedBy() { return releasedBy; }
  public void setReleasedBy(String releasedBy) { this.releasedBy = releasedBy; }

  public Long getReleasedTs() { return releasedTs; }
  public void setReleasedTs(Long releasedTs) { this.releasedTs = releasedTs; }

  public String getReleaseReason() { return releaseReason; }
  public void setReleaseReason(String releaseReason) { this.releaseReason = releaseReason; }

  public String getStatus() { return status; }
  public void setStatus(String status) { this.status = status; }

  /** Status constants. */
  public static final String STATUS_RUNNING        = "RUNNING";
  public static final String STATUS_COMPLETED      = "COMPLETED";
  public static final String STATUS_RELEASED       = "RELEASED";
  public static final String STATUS_FORCE_RELEASED = "FORCE_RELEASED";
}
