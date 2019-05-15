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

package org.apache.hadoop.hive.metastore.model;

import java.util.Set;

public class MWMTrigger {
  private MWMResourcePlan resourcePlan;
  private String name;
  private String triggerExpression;
  private String actionExpression;
  // This is integer because in Derby DN converts boolean to char, breaking sysdb.
  private int isInUnmanaged;
  private Set<MWMPool> pools;

  public MWMTrigger(MWMResourcePlan resourcePlan, String name, String triggerExpression,
      String actionExpression, Set<MWMPool> pools, boolean isInUnmanaged) {
    this.resourcePlan = resourcePlan;
    this.name = name;
    this.triggerExpression = triggerExpression;
    this.actionExpression = actionExpression;
    this.pools = pools;
    this.isInUnmanaged = isInUnmanaged ? 1 : 0;
  }

  public MWMResourcePlan getResourcePlan() {
    return resourcePlan;
  }

  public void setResourcePlan(MWMResourcePlan resourcePlan) {
    this.resourcePlan = resourcePlan;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTriggerExpression() {
    return triggerExpression;
  }

  public void setTriggerExpression(String triggerExpression) {
    this.triggerExpression = triggerExpression;
  }

  public String getActionExpression() {
    return actionExpression;
  }

  public void setActionExpression(String actionExpression) {
    this.actionExpression = actionExpression;
  }

  public Set<MWMPool> getPools() {
    return pools;
  }

  public void setPools(Set<MWMPool> pools) {
    this.pools = pools;
  }

  public boolean getIsInUnmanaged() {
    return isInUnmanaged == 1;
  }

  public void setIsInUnmanaged(boolean isInUnmanaged) {
    this.isInUnmanaged = isInUnmanaged ? 1 : 0;
  }
}
