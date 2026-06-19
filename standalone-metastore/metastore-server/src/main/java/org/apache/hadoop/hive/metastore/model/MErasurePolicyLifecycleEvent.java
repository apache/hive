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

/** Append-only audit-log entry for lifecycle transitions and attach attempts. */
public class MErasurePolicyLifecycleEvent {

  private MErasurePolicyVersion version;
  private String eventType;             // VALIDATED | ACTIVATED | DEACTIVATED | SUPERSEDED | BOUND | UNBOUND | ATTACH_REJECTED
  private String principal;
  private long   eventTs;
  private String note;
  private String conflictClass;         // only for ATTACH_REJECTED
  private MErasurePolicyBinding binding; // nullable; populated for binding-scoped events

  public MErasurePolicyLifecycleEvent() {
  }

  public MErasurePolicyLifecycleEvent(MErasurePolicyVersion version, String eventType,
                                      String principal, long eventTs, String note,
                                      String conflictClass, MErasurePolicyBinding binding) {
    this.version = version;
    this.eventType = eventType;
    this.principal = principal;
    this.eventTs = eventTs;
    this.note = note;
    this.conflictClass = conflictClass;
    this.binding = binding;
  }

  public MErasurePolicyVersion getVersion() { return version; }
  public void setVersion(MErasurePolicyVersion version) { this.version = version; }

  public String getEventType() { return eventType; }
  public void setEventType(String eventType) { this.eventType = eventType; }

  public String getPrincipal() { return principal; }
  public void setPrincipal(String principal) { this.principal = principal; }

  public long getEventTs() { return eventTs; }
  public void setEventTs(long eventTs) { this.eventTs = eventTs; }

  public String getNote() { return note; }
  public void setNote(String note) { this.note = note; }

  public String getConflictClass() { return conflictClass; }
  public void setConflictClass(String conflictClass) { this.conflictClass = conflictClass; }

  public MErasurePolicyBinding getBinding() { return binding; }
  public void setBinding(MErasurePolicyBinding binding) { this.binding = binding; }
}
