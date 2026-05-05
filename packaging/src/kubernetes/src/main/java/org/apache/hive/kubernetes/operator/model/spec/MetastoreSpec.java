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

package org.apache.hive.kubernetes.operator.model.spec;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;

/** Configuration for the Hive Metastore component. */
public class MetastoreSpec {

  @JsonPropertyDescription("Number of Metastore replicas")
  private int replicas = 1;

  @JsonPropertyDescription("Resource requirements for Metastore pods")
  private ResourceRequirementsSpec resources;

  @JsonPropertyDescription("Database connection configuration for the metastore backend")
  private DatabaseConfig database = new DatabaseConfig();

  @JsonPropertyDescription("Warehouse directory path")
  private String warehouseDir = "/opt/hive/data/warehouse";

  @JsonPropertyDescription(
      "Additional metastore-site.xml configuration overrides as key-value pairs")
  private Map<String, String> configOverrides;

  public int getReplicas() {
    return replicas;
  }

  public void setReplicas(int replicas) {
    this.replicas = replicas;
  }

  public ResourceRequirementsSpec getResources() {
    return resources;
  }

  public void setResources(ResourceRequirementsSpec resources) {
    this.resources = resources;
  }

  public DatabaseConfig getDatabase() {
    return database;
  }

  public void setDatabase(DatabaseConfig database) {
    this.database = database;
  }

  public String getWarehouseDir() {
    return warehouseDir;
  }

  public void setWarehouseDir(String warehouseDir) {
    this.warehouseDir = warehouseDir;
  }

  public Map<String, String> getConfigOverrides() {
    return configOverrides;
  }

  public void setConfigOverrides(Map<String, String> configOverrides) {
    this.configOverrides = configOverrides;
  }
}
