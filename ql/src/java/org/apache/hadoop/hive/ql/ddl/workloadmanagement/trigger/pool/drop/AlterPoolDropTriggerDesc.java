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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.trigger.pool.drop;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER POOL ... DROP TRIGGER commands.
 */
@Explain(displayName = "Drop Trigger to pool mappings", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterPoolDropTriggerDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 383046258694558029L;

  private final String resourcePlanName;
  private final String triggerName;
  private final String poolPath;

  public AlterPoolDropTriggerDesc(String resourcePlanName, String triggerName, String poolPath) {
    this.resourcePlanName = resourcePlanName;
    this.triggerName = triggerName;
    this.poolPath = poolPath;
  }

  @Explain(displayName = "Resource plan name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getResourcePlanName() {
    return resourcePlanName;
  }

  @Explain(displayName = "Trigger name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTriggerName() {
    return triggerName;
  }

  public String getPoolPath() {
    return poolPath;
  }

  @Explain(displayName = "Pool path", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPoolPathForExplain() {
    return poolPath == null ? "<unmanaged queries>" : poolPath;
  }
}
