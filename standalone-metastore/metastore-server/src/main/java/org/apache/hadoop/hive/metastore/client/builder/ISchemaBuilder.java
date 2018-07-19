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
package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SchemaCompatibility;
import org.apache.hadoop.hive.metastore.api.SchemaType;
import org.apache.hadoop.hive.metastore.api.SchemaValidation;

public class ISchemaBuilder {
  private SchemaType schemaType; // required
  private String name; // required
  private String dbName, catName; // required
  private SchemaCompatibility compatibility; // required
  private SchemaValidation validationLevel; // required
  private boolean canEvolve; // required
  private String schemaGroup; // optional
  private String description; // optional

  public ISchemaBuilder() {
    compatibility = SchemaCompatibility.BACKWARD;
    validationLevel = SchemaValidation.ALL;
    canEvolve = true;
    dbName = Warehouse.DEFAULT_DATABASE_NAME;
    catName = Warehouse.DEFAULT_CATALOG_NAME;
  }

  public ISchemaBuilder setSchemaType(SchemaType schemaType) {
    this.schemaType = schemaType;
    return this;
  }

  public ISchemaBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public ISchemaBuilder setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public ISchemaBuilder inDb(Database db) {
    this.catName = db.getCatalogName();
    this.dbName = db.getName();
    return this;
  }

  public ISchemaBuilder setCompatibility(SchemaCompatibility compatibility) {
    this.compatibility = compatibility;
    return this;
  }

  public ISchemaBuilder setValidationLevel(SchemaValidation validationLevel) {
    this.validationLevel = validationLevel;
    return this;
  }

  public ISchemaBuilder setCanEvolve(boolean canEvolve) {
    this.canEvolve = canEvolve;
    return this;
  }

  public ISchemaBuilder setSchemaGroup(String schemaGroup) {
    this.schemaGroup = schemaGroup;
    return this;
  }

  public ISchemaBuilder setDescription(String description) {
    this.description = description;
    return this;
  }

  public ISchema build() throws MetaException {
    if (schemaType == null || name == null) {
      throw new MetaException("You must provide a schemaType and name");
    }
    ISchema iSchema =
        new ISchema(schemaType, name, catName, dbName, compatibility, validationLevel, canEvolve);
    if (schemaGroup != null) iSchema.setSchemaGroup(schemaGroup);
    if (description != null) iSchema.setDescription(description);
    return iSchema;
  }
}
