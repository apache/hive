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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.api;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hcatalog.common.HCatException;

/**
 * The Class HCatCreateDBDesc for defining database attributes.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.api.HCatCreateDBDesc} instead
 */
public class HCatCreateDBDesc {

  private String dbName;
  private String locationUri;
  private String comment;
  private Map<String, String> dbProperties;
  private boolean ifNotExits = false;

  /**
   * Gets the database properties.
   *
   * @return the database properties
   */
  public Map<String, String> getDatabaseProperties() {
    return this.dbProperties;
  }

  /**
   * Gets the if not exists.
   *
   * @return the if not exists
   */
  public boolean getIfNotExists() {
    return this.ifNotExits;
  }

  /**
   * Gets the comments.
   *
   * @return the comments
   */
  public String getComments() {
    return this.comment;
  }

  /**
   * Gets the location.
   *
   * @return the location
   */
  public String getLocation() {
    return this.locationUri;
  }

  /**
   * Gets the database name.
   *
   * @return the database name
   */
  public String getDatabaseName() {
    return this.dbName;
  }

  private HCatCreateDBDesc(String dbName) {
    this.dbName = dbName;
  }

  @Override
  public String toString() {
    return "HCatCreateDBDesc ["
      + (dbName != null ? "dbName=" + dbName + ", " : "dbName=null")
      + (locationUri != null ? "location=" + locationUri + ", "
      : "location=null")
      + (comment != null ? "comment=" + comment + ", " : "comment=null")
      + (dbProperties != null ? "dbProperties=" + dbProperties + ", "
      : "dbProperties=null") + "ifNotExits=" + ifNotExits + "]";
  }

  /**
   * Creates the builder for defining attributes.
   *
   * @param dbName the db name
   * @return the builder
   */
  public static Builder create(String dbName) {
    return new Builder(dbName);
  }

  Database toHiveDb() {
    Database hiveDB = new Database();
    hiveDB.setDescription(this.comment);
    hiveDB.setLocationUri(this.locationUri);
    hiveDB.setName(this.dbName);
    hiveDB.setParameters(this.dbProperties);
    return hiveDB;
  }

  public static class Builder {

    private String innerLoc;
    private String innerComment;
    private Map<String, String> innerDBProps;
    private String dbName;
    private boolean ifNotExists = false;

    private Builder(String dbName) {
      this.dbName = dbName;
    }

    /**
     * Location.
     *
     * @param value the location of the database.
     * @return the builder
     */
    public Builder location(String value) {
      this.innerLoc = value;
      return this;
    }

    /**
     * Comment.
     *
     * @param value comments.
     * @return the builder
     */
    public Builder comment(String value) {
      this.innerComment = value;
      return this;
    }

    /**
     * If not exists.
     * @param ifNotExists If set to true, hive will not throw exception, if a
     * database with the same name already exists.
     * @return the builder
     */
    public Builder ifNotExists(boolean ifNotExists) {
      this.ifNotExists = ifNotExists;
      return this;
    }

    /**
     * Database properties.
     *
     * @param dbProps the database properties
     * @return the builder
     */
    public Builder databaseProperties(Map<String, String> dbProps) {
      this.innerDBProps = dbProps;
      return this;
    }


    /**
     * Builds the create database descriptor.
     *
     * @return An instance of HCatCreateDBDesc
     * @throws HCatException
     */
    public HCatCreateDBDesc build() throws HCatException {
      if (this.dbName == null) {
        throw new HCatException("Database name cannot be null.");
      }
      HCatCreateDBDesc desc = new HCatCreateDBDesc(this.dbName);
      desc.comment = this.innerComment;
      desc.locationUri = this.innerLoc;
      desc.dbProperties = this.innerDBProps;
      desc.ifNotExits = this.ifNotExists;
      return desc;

    }

  }

}
