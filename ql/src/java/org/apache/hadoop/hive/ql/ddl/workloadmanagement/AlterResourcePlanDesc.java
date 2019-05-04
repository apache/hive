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

import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask2;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER RESOURCE PLAN commands.
 */
@Explain(displayName = "Alter Resource plans", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterResourcePlanDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = -3514685833183437279L;

  static {
    DDLTask2.registerOperation(AlterResourcePlanDesc.class, AlterResourcePlanOperation.class);
  }

  public static final String SCHEMA = "error#string";

  private final WMNullableResourcePlan resourcePlan;
  private final String planName;
  private final boolean validate;
  private final boolean isEnableActivate;
  private final boolean isForceDeactivate;
  private final boolean isReplace;
  private final String resFile;

  public AlterResourcePlanDesc(WMNullableResourcePlan resourcePlan, String planName, boolean validate,
      boolean isEnableActivate, boolean isForceDeactivate, boolean isReplace, String resFile) {
    this.resourcePlan = resourcePlan;
    this.planName = planName;
    this.validate = validate;
    this.isEnableActivate = isEnableActivate;
    this.isForceDeactivate = isForceDeactivate;
    this.isReplace = isReplace;
    this.resFile = resFile;
  }

  @Explain(displayName="Resource plan changed fields", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public WMNullableResourcePlan getResourcePlan() {
    return resourcePlan;
  }

  @Explain(displayName="Resource plan to modify", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPlanName() {
    return planName;
  }

  @Explain(displayName="shouldValidate", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean shouldValidate() {
    return validate;
  }

  public boolean isEnableActivate() {
    return isEnableActivate;
  }

  public boolean isForceDeactivate() {
    return isForceDeactivate;
  }

  public boolean isReplace() {
    return isReplace;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }
}
