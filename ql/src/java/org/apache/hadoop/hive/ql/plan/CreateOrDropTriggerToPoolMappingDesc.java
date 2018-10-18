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
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Create/Drop Trigger to pool mappings",
    explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateOrDropTriggerToPoolMappingDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 383046258694558029L;

  private String resourcePlanName;
  private String triggerName;
  private String poolPath;
  private boolean isUnmanagedPool;
  private boolean drop;

  public CreateOrDropTriggerToPoolMappingDesc() {}

  public CreateOrDropTriggerToPoolMappingDesc(String resourcePlanName, String triggerName,
      String poolPath, boolean drop, boolean isUnmanagedPool) {
    this.resourcePlanName = resourcePlanName;
    this.triggerName = triggerName;
    this.poolPath = poolPath;
    this.isUnmanagedPool = isUnmanagedPool;
    this.drop = drop;
  }

  @Explain(displayName = "resourcePlanName",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getResourcePlanName() {
    return resourcePlanName;
  }

  public void setResourcePlanName(String resourcePlanName) {
    this.resourcePlanName = resourcePlanName;
  }

  @Explain(displayName = "Trigger name",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTriggerName() {
    return triggerName;
  }

  public void setTriggerName(String triggerName) {
    this.triggerName = triggerName;
  }

  @Explain(displayName = "Pool path",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPoolPathForExplain() {
    return isUnmanagedPool ? "<unmanaged queries>" : poolPath;
  }

  public String getPoolPath() {
    return poolPath;
  }

  public boolean isUnmanagedPool() {
    return isUnmanagedPool;
  }

  public void setPoolPath(String poolPath) {
    this.poolPath = poolPath;
  }

  @Explain(displayName = "drop or create",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean shouldDrop() {
    return drop;
  }

  public void setDrop(boolean drop) {
    this.drop = drop;
  }
}
