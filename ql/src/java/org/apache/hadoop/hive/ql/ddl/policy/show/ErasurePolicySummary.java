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

package org.apache.hadoop.hive.ql.ddl.policy.show;

import java.io.Serializable;

/**
 * One row of {@code SHOW ERASURE POLICIES}: a policy name plus a compact view
 * of its version lifecycle. This replaces dumping the raw multi-line {@code .erp}
 * source into the listing (use {@code DESCRIBE ERASURE POLICY} for the source);
 * keeping every field single-line guarantees one clean row per policy.
 */
public class ErasurePolicySummary implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String policyName;
  private final String activeVersion;
  private final String versions;

  public ErasurePolicySummary(String policyName, String activeVersion, String versions) {
    this.policyName = policyName;
    this.activeVersion = activeVersion;
    this.versions = versions;
  }

  /** The policy name. */
  public String getPolicyName() {
    return policyName;
  }

  /** Label of the version currently in the {@code ACTIVE} state, or {@code -} if none. */
  public String getActiveVersion() {
    return activeVersion;
  }

  /** Compact {@code label(STATUS)} list of every version, oldest first. */
  public String getVersions() {
    return versions;
  }
}
