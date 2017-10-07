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

import java.util.List;

/**
 * Storage class for ResourcePlan.
 */
public class MWMResourcePlan {
  private String name;
  private Integer queryParallelism;
  private Status status;
  private List<MWMPool> pools;
  private List<MWMTrigger> triggers;
  private List<MWMMapping> mappings;

  public enum Status {
    ACTIVE,
    ENABLED,
    DISABLED
  }

  public MWMResourcePlan() {}

  public MWMResourcePlan(String name, Integer queryParallelism, Status status) {
    this.name = name;
    this.queryParallelism = queryParallelism;
    this.status = status;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getQueryParallelism() {
    return queryParallelism;
  }

  public void setQueryParallelism(Integer queryParallelism) {
    this.queryParallelism = queryParallelism;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public List<MWMPool> getPools() {
    return pools;
  }

  public void setPools(List<MWMPool> pools) {
    this.pools = pools;
  }

  public List<MWMTrigger> getTriggers() {
    return triggers;
  }

  public void setTriggers(List<MWMTrigger> triggers) {
    this.triggers = triggers;
  }

  public List<MWMMapping> getMappings() {
    return mappings;
  }

  public void setMappings(List<MWMMapping> mappings) {
    this.mappings = mappings;
  }
}
