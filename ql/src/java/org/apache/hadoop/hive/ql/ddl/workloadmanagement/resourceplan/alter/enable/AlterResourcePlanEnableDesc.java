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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.alter.enable;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER RESOURCE PLAN ... ENABLE or ENABLE WORKLOAD MANAGEMENT commands.
 */
@Explain(displayName = "Enable Resource plan", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterResourcePlanEnableDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String resourcePlanName;
  private final boolean enable;
  private final boolean activate;
  private final boolean replace;

  public AlterResourcePlanEnableDesc(String resourcePlanName, boolean enable, boolean activate, boolean replace) {
    this.resourcePlanName = resourcePlanName;
    this.enable = enable;
    this.activate = activate;
    this.replace = replace;
  }

  @Explain(displayName="Resource plan name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getResourcePlanName() {
    return resourcePlanName;
  }

  @Explain(displayName="Enable", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      displayOnlyOnTrue=true)
  public boolean isEnable() {
    return enable;
  }

  @Explain(displayName="Activate", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      displayOnlyOnTrue=true)
  public boolean isActivate() {
    return activate;
  }

  @Explain(displayName="Replace", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      displayOnlyOnTrue=true)
  public boolean isReplace() {
    return replace;
  }
}
