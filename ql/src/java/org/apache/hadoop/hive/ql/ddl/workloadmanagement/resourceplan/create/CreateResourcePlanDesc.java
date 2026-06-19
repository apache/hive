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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.create;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for CREATE RESOURCE PLAN commands.
 */
@Explain(displayName = "Create Resource plan", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateResourcePlanDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = -3492803425541479414L;

  private final String resourcePlanName;
  private final Integer queryParallelism;
  private final String copyFromName;
  private final boolean ifNotExists;

  public CreateResourcePlanDesc(String resourcePlanName, Integer queryParallelism, String copyFromName,
      boolean ifNotExists) {
    this.resourcePlanName = resourcePlanName;
    this.queryParallelism = queryParallelism;
    this.copyFromName = copyFromName;
    this.ifNotExists = ifNotExists;
  }

  @Explain(displayName="Resource plan name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getResourcePlanName() {
    return resourcePlanName;
  }

  @Explain(displayName="Query parallelism", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Integer getQueryParallelism() {
    return queryParallelism;
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
