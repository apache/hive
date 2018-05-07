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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Show Resource plans",
    explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowResourcePlanDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 6076076933035978545L;

  private static final String TABLE = "show_resourceplan";
  private static final String ALL_SCHEMA = "rp_name,status,query_parallelism#string,string,int";
  private static final String SINGLE_SCHEMA = "line#string";

  String resFile;
  String resourcePlanName;

  // For serialization only.
  public ShowResourcePlanDesc() {}

  public ShowResourcePlanDesc(String rpName, Path resFile) {
    this.resourcePlanName = rpName;
    this.resFile = resFile.toString();
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  public void setResFile(String resFile) {
    this.resFile = resFile;
  }

  @Explain(displayName="resourcePlanName",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getResourcePlanName() {
    return resourcePlanName;
  }

  public String getTable() {
    return TABLE;
  }

  public String getSchema(String rpName) {
    return (rpName == null) ? ALL_SCHEMA : SINGLE_SCHEMA;
  }
}
