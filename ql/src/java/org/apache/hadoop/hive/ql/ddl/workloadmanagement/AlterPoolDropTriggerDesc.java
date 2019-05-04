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
package org.apache.hadoop.hive.ql.ddl.workloadmanagement;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask2;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER POOL ... DROP TRIGGER commands.
 */
@Explain(displayName = "Drop Trigger to pool mappings", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterPoolDropTriggerDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 383046258694558029L;

  static {
    DDLTask2.registerOperation(AlterPoolDropTriggerDesc.class, AlterPoolDropTriggerOperation.class);
  }

  private final String planName;
  private final String triggerName;
  private final String poolPath;
  private final boolean isUnmanagedPool;

  public AlterPoolDropTriggerDesc(String planName, String triggerName, String poolPath, boolean isUnmanagedPool) {
    this.planName = planName;
    this.triggerName = triggerName;
    this.poolPath = poolPath;
    this.isUnmanagedPool = isUnmanagedPool;
  }

  @Explain(displayName = "resourcePlanName", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPlanName() {
    return planName;
  }

  @Explain(displayName = "Trigger name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTriggerName() {
    return triggerName;
  }

  @Explain(displayName = "Pool path", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPoolPathForExplain() {
    return isUnmanagedPool ? "<unmanaged queries>" : poolPath;
  }

  public String getPoolPath() {
    return poolPath;
  }

  public boolean isUnmanagedPool() {
    return isUnmanagedPool;
  }
}
