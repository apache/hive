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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.show;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW RESOURCE PLAN(S) commands.
 */
@Explain(displayName = "Show Resource plans", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowResourcePlanDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 6076076933035978545L;

  private static final String ALL_SCHEMA = "rp_name,status,query_parallelism#string,string,int";
  private static final String SINGLE_SCHEMA = "line#string";

  private final String resourcePlanName;
  private final String resFile;

  public ShowResourcePlanDesc(String resourcePlanName, String resFile) {
    this.resourcePlanName = resourcePlanName;
    this.resFile = resFile;
  }

  @Explain(displayName="Resource plan name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getResourcePlanName() {
    return resourcePlanName;
  }

  @Explain(displayName = "Result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  public String getSchema() {
    return (resourcePlanName == null) ? ALL_SCHEMA : SINGLE_SCHEMA;
  }
}
