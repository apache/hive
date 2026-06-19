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

/**
 *
 */
package org.apache.hadoop.hive.metastore.model;

import org.apache.hadoop.hive.common.repl.ReplConst;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Storage Class representing the Hive MDatabase in a rdbms
 *
 */
public class MDatabase {
  private String name;
  private String locationUri;
  private String managedLocationUri;
  private String description;
  private Map<String, String> parameters;
  private String ownerName;
  private String ownerType;
  private String catalogName;
  private int createTime;
  private String type;
  private String connectorName;
  private String remoteDatabaseName;

  /**
   * Default construction to keep jpox/jdo happy
   */
  public MDatabase() {}

  /**
   * To create a database object
   * @param catalogName Name of the catalog, the database belongs to.
   * @param name of the database
   * @param locationUri Location of the database in the warehouse
   * @param description Comment describing the database
   * @param parameters Parameters for the database
   */
  public MDatabase(String catalogName, String name, String locationUri, String description,
      Map<String, String> parameters) {
    this(catalogName, name, locationUri, description, parameters, null);
  }

  /**
   * To create a database object
   * @param catalogName Name of the catalog, the database belongs to.
   * @param name of the database
   * @param locationUri Default external Location of the database
   * @param description Comment describing the database
   * @param parameters Parameters for the database
   * @param managedLocationUri Default location for managed tables in database in the warehouse
   */
  public MDatabase(String catalogName, String name, String locationUri, String description,
      Map<String, String> parameters, String managedLocationUri) {
    this(catalogName, name, locationUri, description, parameters, null, "NATIVE", null);
  }

    /**
     * To create a database object
     * @param catalogName Name of the catalog, the database belongs to.
     * @param name of the database
     * @param locationUri Default external Location of the database
     * @param description Comment describing the database
     * @param parameters Parameters for the database
     * @param managedLocationUri Default location for managed tables in database in the warehouse
     */
  public MDatabase(String catalogName, String name, String locationUri, String description,
        Map<String, String> parameters, String managedLocationUri, String type, String dcName) {
    this.name = name;
    this.locationUri = locationUri;
    this.managedLocationUri = managedLocationUri;
    this.description = description;
    this.parameters = parameters;
    this.catalogName = catalogName;
    this.type = type;
    this.connectorName = dcName;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the location_uri
   */
  public String getLocationUri() {
    return locationUri;
  }

  /**
   * @param locationUri the locationUri to set
   */
  public void setLocationUri(String locationUri) {
    this.locationUri = locationUri;
  }

  /**
   * @return the managedLocationUri
   */
  public String getManagedLocationUri() {
    return managedLocationUri;
  }

  /**
   * @param managedLocationUri the locationUri to set for managed tables.
   */
  public void setManagedLocationUri(String managedLocationUri) {
    this.managedLocationUri = managedLocationUri;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description the description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * @return the parameters mapping.
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters mapping.
   */
  public void setParameters(Map<String, String> parameters) {
    if (parameters == null) {
      this.parameters = null;
      return;
    }
    this.parameters = new HashMap<>();
    Set<String> keys = new HashSet<>(parameters.keySet());
    for(String key : keys) {
      // Normalize the case for source of replication parameter
      if (ReplConst.SOURCE_OF_REPLICATION.equalsIgnoreCase(key)) {
        // TODO :  Some extra validation can also be added as this is a user provided parameter.
        this.parameters.put(ReplConst.SOURCE_OF_REPLICATION, parameters.get(key));
      } else {
        this.parameters.put(key, parameters.get(key));
      }
    }
  }

  public String getOwnerName() {
    return ownerName;
  }

  public void setOwnerName(String ownerName) {
    this.ownerName = ownerName;
  }

  public String getOwnerType() {
    return ownerType;
  }

  public void setOwnerType(String ownerType) {
    this.ownerType = ownerType;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public void setCatalogName(String catalogName) {
    this.catalogName = catalogName;
  }

  public int getCreateTime() {
    return createTime;
  }

  public void setCreateTime(int createTime) {
    this.createTime = createTime;
  }

  /**
   * @return the name
   */
  public String getType() {
    return type;
  }

  /**
   * @param type the type of the database to set
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * @return the name
   */
  public String getDataConnectorName() {
    return connectorName;
  }

  /**
   * @param dcName the name of the dataconnector to set
   */
  public void setDataConnectorName(String dcName) {
    this.connectorName = dcName;
  }

  /**
   * @return the remote database name this db maps to
   */
  public String getRemoteDatabaseName() {
    return remoteDatabaseName;
  }

  /**
   * @param remoteDBName the name of the dataconnector to set
   */
  public void setRemoteDatabaseName(String remoteDBName) {
    this.remoteDatabaseName = remoteDBName;
  }

}
