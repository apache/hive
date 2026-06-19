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

import org.apache.hadoop.hive.metastore.ReplChangeManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Storage Class representing the Hive DataConnector in a rdbms
 *
 */
public class MDataConnector {
  private String name;
  private String type;
  private String url;
  private String description;
  private Map<String, String> parameters;
  private String ownerName;
  private String ownerType;
  private int createTime;

  /**
   * Default construction to keep jpox/jdo happy
   */
  public MDataConnector() {}

  /**
   * To create a dataconnector object
   * @param name of the dataconnector
   * @param description Comment describing the dataconnector
   * @param parameters Parameters for the dataconnector
   */
  public MDataConnector(String name, String type, String url, String description, Map<String, String> parameters) {
    this(name, type, url, description, parameters, null);
  }

  /**
   * To create a dataconnector object
   * @param name of the dataconnector
   * @param type of the dataconnector
   * @param description Comment describing the dataconnector
   * @param parameters Parameters for the dataconnector
   */
  public MDataConnector(String name, String type, String url, String description,
      Map<String, String> parameters, String TODO) {
    this.name = name;
    this.type = type;
    this.url  = url;
    this.description = description;
    this.parameters = parameters;
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
  public String getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  /**
   * @param url the url to set
   */
  public void setUrl(String url) {
    this.url = url;
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
      this.parameters.put(key, parameters.get(key));
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

  public int getCreateTime() {
    return createTime;
  }

  public void setCreateTime(int createTime) {
    this.createTime = createTime;
  }
}
