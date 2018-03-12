/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.model;

public class MISchema {
  private int schemaType;
  private String name;
  private MDatabase db;
  private int compatibility;
  private int validationLevel;
  private boolean canEvolve;
  private String schemaGroup;
  private String description;

  public MISchema(int schemaType, String name, MDatabase db, int compatibility,
                  int validationLevel, boolean canEvolve, String schemaGroup, String description) {
    this.schemaType = schemaType;
    this.name = name;
    this.db= db;
    this.compatibility = compatibility;
    this.validationLevel = validationLevel;
    this.canEvolve = canEvolve;
    this.schemaGroup = schemaGroup;
    this.description = description;
  }

  public int getSchemaType() {
    return schemaType;
  }

  public void setSchemaType(int schemaType) {
    this.schemaType = schemaType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public MDatabase getDb() {
    return db;
  }

  public MISchema setDb(MDatabase db) {
    this.db = db;
    return this;
  }

  public int getCompatibility() {
    return compatibility;
  }

  public void setCompatibility(int compatibility) {
    this.compatibility = compatibility;
  }

  public int getValidationLevel() {
    return validationLevel;
  }

  public void setValidationLevel(int validationLevel) {
    this.validationLevel = validationLevel;
  }

  public boolean getCanEvolve() {
    return canEvolve;
  }

  public void setCanEvolve(boolean canEvolve) {
    this.canEvolve = canEvolve;
  }

  public String getSchemaGroup() {
    return schemaGroup;
  }

  public void setSchemaGroup(String schemaGroup) {
    this.schemaGroup = schemaGroup;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}

