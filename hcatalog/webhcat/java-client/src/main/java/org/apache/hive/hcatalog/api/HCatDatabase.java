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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api;

import java.util.Map;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.Database;


/**
 * HCatDatabase is wrapper class around org.apache.hadoop.hive.metastore.api.Database.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HCatDatabase {

  private String dbName;
  private String dbLocation;
  private String comment;
  private Map<String, String> props;

  HCatDatabase(Database db) {
    this.dbName = db.getName();
    this.props = db.getParameters();
    this.dbLocation = db.getLocationUri();
    this.comment = db.getDescription();
  }

  /**
   * Gets the database name.
   *
   * @return the database name
   */
  public String getName() {
    return dbName;
  }

  /**
   * Gets the dB location.
   *
   * @return the dB location
   */
  public String getLocation() {
    return dbLocation;
  }

  /**
   * Gets the comment.
   *
   * @return the comment
   */
  public String getComment() {
    return comment;
  }

  /**
   * Gets the dB properties.
   *
   * @return the dB properties
   */
  public Map<String, String> getProperties() {
    return props;
  }

  @Override
  public String toString() {
    return "HCatDatabase ["
      + (dbName != null ? "dbName=" + dbName + ", " : "dbName=null")
      + (dbLocation != null ? "dbLocation=" + dbLocation + ", " : "dbLocation=null")
      + (comment != null ? "comment=" + comment + ", " : "comment=null")
      + (props != null ? "props=" + props : "props=null") + "]";
  }

}
