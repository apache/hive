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

package org.apache.hadoop.hive.ql.ddl.policy.desc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;

/**
 * DDL task description for DESC ERASURE POLICY commands.
 */
@Explain(displayName = "Describe Erasure Policy", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DescErasurePolicyDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  // A policy description is rendered as a single text column: a short governance
  // header followed by the policy source, one line per row. A multi-column schema
  // cannot carry the multi-line source without its newlines splitting every line
  // into a bogus row (with NULLs in the other column).
  public static final String DESC_ERASURE_POLICY_SCHEMA =
      "erasure_policy#string";

  private final String resFile;
  private final String policyName;
  private final String versionLabel;
  private final boolean isExtended;

  public DescErasurePolicyDesc(Path resFile, String policyName, String versionLabel, boolean isExtended) {
    this.resFile = resFile.toString();
    this.policyName = policyName;
    this.versionLabel = versionLabel;
    this.isExtended = isExtended;
  }

  @Explain(displayName = "extended", displayOnlyOnTrue=true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isExtended() {
    return isExtended;
  }

  @Explain(displayName = "policy", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPolicyName() {
    return policyName;
  }

  /** Optional version label from {@code DESCRIBE ERASURE POLICY p VERSION 'label'}; null selects the active version. */
  public String getVersionLabel() {
    return versionLabel;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  public String getSchema() {
    return DESC_ERASURE_POLICY_SCHEMA;
  }
}
