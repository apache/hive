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

package org.apache.hadoop.hive.metastore.model;

import java.util.Set;

public class MWMPool {
  private MWMResourcePlan resourcePlan;
  private String path;
  private MWMPool parentPool;
  private Double allocFraction;
  private Integer queryParallelism;
  private Set<MWMTrigger> triggers;

  public MWMPool() {}

  public MWMPool(MWMResourcePlan resourcePlan, String path, MWMPool parentPool,
      Double allocFraction, Integer queryParallelism) {
    this.resourcePlan = resourcePlan;
    this.path = path;
    this.parentPool = parentPool;
    this.allocFraction = allocFraction;
    this.queryParallelism = queryParallelism;
  }

  public MWMResourcePlan getResourcePlan() {
    return resourcePlan;
  }

  public void setResourcePlan(MWMResourcePlan resourcePlan) {
    this.resourcePlan = resourcePlan;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public MWMPool getParentPool() {
    return parentPool;
  }

  public void setParentPool(MWMPool parentPool) {
    this.parentPool = parentPool;
  }

  public Double getAllocFraction() {
    return allocFraction;
  }

  public void setAllocFraction(Double allocFraction) {
    this.allocFraction = allocFraction;
  }

  public Integer getQueryParallelism() {
    return queryParallelism;
  }

  public void setQueryParallelism(Integer queryParallelism) {
    this.queryParallelism = queryParallelism;
  }

  public Set<MWMTrigger> getTriggers() {
    return triggers;
  }

  public void setTriggers(Set<MWMTrigger> triggers) {
    this.triggers = triggers;
  }
}
