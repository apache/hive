/**
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

@Explain(displayName="Create WM Trigger",
    explainLevels={ Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateWMTriggerDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1705317739121300923L;

  private String rpName;
  private String triggerName;
  private String triggerExpression;
  private String actionExpression;

  public CreateWMTriggerDesc() {}

  public CreateWMTriggerDesc(String rpName, String triggerName, String triggerExpression,
      String actionExpression) {
    this.rpName = rpName;
    this.triggerName = triggerName;
    this.triggerExpression = triggerExpression;
    this.actionExpression = actionExpression;
  }

  @Explain(displayName="resourcePlanName",
      explainLevels={ Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getRpName() {
    return rpName;
  }

  public void setRpName(String rpName) {
    this.rpName = rpName;
  }

  @Explain(displayName="triggerName",
      explainLevels={ Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTriggerName() {
    return triggerName;
  }

  public void setTriggerName(String triggerName) {
    this.triggerName = triggerName;
  }

  @Explain(displayName="triggerExpression",
      explainLevels={ Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTriggerExpression() {
    return triggerExpression;
  }

  public void setTriggerExpression(String triggerExpression) {
    this.triggerExpression = triggerExpression;
  }

  @Explain(displayName="actionExpression",
      explainLevels={ Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getActionExpression() {
    return actionExpression;
  }

  public void setActionExpression(String actionExpression) {
    this.actionExpression = actionExpression;
  }
}
