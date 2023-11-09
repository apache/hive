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
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Create ResourcePlan", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateResourcePlanDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = -3492803425541479414L;

  private WMResourcePlan resourcePlan;
  private String copyFromName;
  private boolean ifNotExists;

  // For serialization only.
  public CreateResourcePlanDesc() {
  }

  public CreateResourcePlanDesc(String planName, Integer queryParallelism, String copyFromName,
      boolean ifNotExists) {
    resourcePlan = new WMResourcePlan(planName);
    if (queryParallelism != null) {
      resourcePlan.setQueryParallelism(queryParallelism);
    }
    this.copyFromName = copyFromName;
    this.ifNotExists = ifNotExists;
  }

  @Explain(displayName="resourcePlan", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public WMResourcePlan getResourcePlan() {
    return resourcePlan;
  }

  @Explain(displayName="Copy from", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getCopyFromName() {
    return copyFromName;
  }

  @Explain(displayName="If not exists", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }
}