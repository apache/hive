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

public class MWMMapping {
  private MWMResourcePlan resourcePlan;
  private EntityType entityType;
  private String entityName;
  private MWMPool pool;
  private Integer ordering;

  public enum EntityType {
    USER,
    GROUP,
    APPLICATION
  }

  public MWMMapping(MWMResourcePlan resourcePlan, EntityType entityType, String entityName,
      MWMPool pool, Integer ordering) {
    this.resourcePlan = resourcePlan;
    this.entityType = entityType;
    this.entityName = entityName;
    this.pool = pool;
    this.ordering = ordering;
  }

  public MWMResourcePlan getResourcePlan() {
    return resourcePlan;
  }

  public void setResourcePlan(MWMResourcePlan resourcePlan) {
    this.resourcePlan = resourcePlan;
  }

  public EntityType getEntityType() {
    return entityType;
  }

  public void setEntityType(EntityType entityType) {
    this.entityType = entityType;
  }

  public String getEntityName() {
    return entityName;
  }

  public void setEntityName(String entityName) {
    this.entityName = entityName;
  }

  public MWMPool getPool() {
    return pool;
  }

  public void setPool(MWMPool pool) {
    this.pool = pool;
  }

  public Integer getOrdering() {
    return ordering;
  }

  public void setOrdering(Integer ordering) {
    this.ordering = ordering;
  }

}
