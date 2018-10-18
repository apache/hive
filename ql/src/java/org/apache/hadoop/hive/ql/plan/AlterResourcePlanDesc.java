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

import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Alter Resource plans",
    explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterResourcePlanDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = -3514685833183437279L;

  private WMNullableResourcePlan resourcePlan;
  private String rpName;
  private boolean validate;
  private boolean isEnableActivate, isForceDeactivate, isReplace;
  private String resFile;

  public AlterResourcePlanDesc() {}

  public AlterResourcePlanDesc(WMNullableResourcePlan resourcePlan, String rpName, boolean validate,
      boolean isEnableActivate, boolean isForceDeactivate, boolean isReplace) {
    this.resourcePlan = resourcePlan;
    this.rpName = rpName;
    this.validate = validate;
    this.isEnableActivate = isEnableActivate;
    this.isForceDeactivate = isForceDeactivate;
    this.isReplace = isReplace;
  }

  @Explain(displayName="Resource plan changed fields",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public WMNullableResourcePlan getResourcePlan() {
    return resourcePlan;
  }

  public void setResourcePlan(WMNullableResourcePlan resourcePlan) {
    this.resourcePlan = resourcePlan;
  }

  @Explain(displayName="Resource plan to modify",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getResourcePlanName() {
    return rpName;
  }

  public void setResourcePlanName(String rpName) {
    this.rpName = rpName;
  }

  @Explain(displayName="shouldValidate",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean shouldValidate() {
    return validate;
  }

  public void setValidate(boolean validate) {
    this.validate = validate;
  }

  public boolean isEnableActivate() {
    return isEnableActivate;
  }

  public void setIsEnableActivate(boolean b) {
    this.isEnableActivate = b;
  }

  public boolean isForceDeactivate() {
    return isForceDeactivate;
  }

  public void setIsForceDeactivate(boolean b) {
    this.isForceDeactivate = b;
  }

  public boolean isReplace() {
    return isReplace;
  }

  public void setIsReplace(boolean b) {
    this.isReplace = b;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  public void setResFile(String resFile) {
    this.resFile = resFile;
  }

  public static String getSchema() {
    return "error#string";
  }
}
