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

/** Per-execution audit of {@code ERASE FROM TABLE} runs. */
public class MErasureRunAudit {

  private long   tblId;
  private String columnName;
  private MErasurePolicyBinding binding;
  private String principal;
  private long   startedTs;
  private Long   completedTs;
  private String identityValues;        // typically hashed
  private String policyVersions;        // JSON array of version_ids
  private Long   resolvedRulesSnapshotId;
  private Integer filesRewritten;
  private Long   bytesBefore;
  private Long   bytesAfter;
  private String status;                // INITIATED -> SUCCEEDED | FAILED | INTERRUPTED

  // INSPECT/FLAG counters. Default to zero so existing audit rows that
  // never carried these columns deserialise cleanly.
  private Long   matchesInspected;
  private Long   matchesRedacted;
  private Long   matchesFlagged;

  // Populated when status is RELEASED or FORCE_RELEASED — carries the
  // WITH REASON '<text>' string from a RELEASE ERASURE LOCK command, so
  // the release event is visible in AUDIT ERASURE EXECUTIONS alongside
  // regular ERASE rows (compliance reviewers see the full lifecycle in
  // one query).
  private String releaseReason;

  public MErasureRunAudit() {
  }

  public MErasureRunAudit(long tblId, String columnName, MErasurePolicyBinding binding,
                          String principal, long startedTs, Long completedTs,
                          String identityValues, String policyVersions,
                          Long resolvedRulesSnapshotId, Integer filesRewritten,
                          Long bytesBefore, Long bytesAfter, String status) {
    this.tblId = tblId;
    this.columnName = columnName;
    this.binding = binding;
    this.principal = principal;
    this.startedTs = startedTs;
    this.completedTs = completedTs;
    this.identityValues = identityValues;
    this.policyVersions = policyVersions;
    this.resolvedRulesSnapshotId = resolvedRulesSnapshotId;
    this.filesRewritten = filesRewritten;
    this.bytesBefore = bytesBefore;
    this.bytesAfter = bytesAfter;
    this.status = status;
  }

  public long getTblId() { return tblId; }
  public void setTblId(long tblId) { this.tblId = tblId; }

  public String getColumnName() { return columnName; }
  public void setColumnName(String columnName) { this.columnName = columnName; }

  public MErasurePolicyBinding getBinding() { return binding; }
  public void setBinding(MErasurePolicyBinding binding) { this.binding = binding; }

  public String getPrincipal() { return principal; }
  public void setPrincipal(String principal) { this.principal = principal; }

  public long getStartedTs() { return startedTs; }
  public void setStartedTs(long startedTs) { this.startedTs = startedTs; }

  public Long getCompletedTs() { return completedTs; }
  public void setCompletedTs(Long completedTs) { this.completedTs = completedTs; }

  public String getIdentityValues() { return identityValues; }
  public void setIdentityValues(String identityValues) { this.identityValues = identityValues; }

  public String getPolicyVersions() { return policyVersions; }
  public void setPolicyVersions(String policyVersions) { this.policyVersions = policyVersions; }

  public Long getResolvedRulesSnapshotId() { return resolvedRulesSnapshotId; }
  public void setResolvedRulesSnapshotId(Long resolvedRulesSnapshotId) { this.resolvedRulesSnapshotId = resolvedRulesSnapshotId; }

  public Integer getFilesRewritten() { return filesRewritten; }
  public void setFilesRewritten(Integer filesRewritten) { this.filesRewritten = filesRewritten; }

  public Long getBytesBefore() { return bytesBefore; }
  public void setBytesBefore(Long bytesBefore) { this.bytesBefore = bytesBefore; }

  public Long getBytesAfter() { return bytesAfter; }
  public void setBytesAfter(Long bytesAfter) { this.bytesAfter = bytesAfter; }

  public String getStatus() { return status; }
  public void setStatus(String status) { this.status = status; }

  public Long getMatchesInspected() { return matchesInspected; }
  public void setMatchesInspected(Long matchesInspected) { this.matchesInspected = matchesInspected; }

  public Long getMatchesRedacted() { return matchesRedacted; }
  public void setMatchesRedacted(Long matchesRedacted) { this.matchesRedacted = matchesRedacted; }

  public Long getMatchesFlagged() { return matchesFlagged; }
  public void setMatchesFlagged(Long matchesFlagged) { this.matchesFlagged = matchesFlagged; }

  public String getReleaseReason() { return releaseReason; }
  public void setReleaseReason(String releaseReason) { this.releaseReason = releaseReason; }
}
