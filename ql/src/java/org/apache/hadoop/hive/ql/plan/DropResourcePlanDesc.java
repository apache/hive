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

@Explain(displayName = "Drop Resource plans", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropResourcePlanDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1258596919510047766L;

  private String rpName;
  private boolean ifExists;

  public DropResourcePlanDesc() {}

  public DropResourcePlanDesc(String rpName, boolean ifExists) {
    this.setRpName(rpName);
    this.setIfExists(ifExists);
  }

  @Explain(displayName="resourcePlanName", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getRpName() {
    return rpName;
  }

  @Explain(displayName="ifExists", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      displayOnlyOnTrue = true)
  public boolean getIfExists() {
    return ifExists;
  }

  public void setRpName(String rpName) {
    this.rpName = rpName;
  }

  public void setIfExists(boolean ifExists) {
    this.ifExists = ifExists;
  }

}