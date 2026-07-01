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
 */
package org.apache.hadoop.hive.ql.ddl.policy.show;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;

/**
 * DDL task description for {@code SHOW ERASURE POLICY <name> HISTORY}.
 * Drives a per-event listing of the policy's lifecycle journal, ordered
 * by transition time.
 */
@Explain(displayName = "Show Erasure Policy History",
    explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowErasurePolicyHistoryDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SHOW_ERASURE_POLICY_HISTORY_SCHEMA =
      "version_id,version_label,version_status,event,event_ts,principal,note"
          + "#bigint:string:string:string:bigint:string:string";

  private final String resFile;
  private final String policyName;

  public ShowErasurePolicyHistoryDesc(Path resFile, String policyName) {
    this.resFile = resFile.toString();
    this.policyName = policyName;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "policy", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPolicyName() {
    return policyName;
  }
}
