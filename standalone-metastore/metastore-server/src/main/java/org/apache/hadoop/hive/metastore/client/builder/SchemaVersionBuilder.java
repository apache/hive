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

import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

public class SchemaVersionBuilder extends SerdeAndColsBuilder<SchemaVersionBuilder> {
  private String schemaName, dbName, catName; // required
  private int version; // required
  private long createdAt; // required
  private SchemaVersionState state; // optional
  private String description; // optional
  private String schemaText; // optional
  private String fingerprint; // optional
  private String name; // optional

  public SchemaVersionBuilder() {
    catName = DEFAULT_CATALOG_NAME;
    dbName = DEFAULT_DATABASE_NAME;
    createdAt = System.currentTimeMillis() / 1000;
    version = -1;
    super.setChild(this);
  }

  public SchemaVersionBuilder setSchemaName(String schemaName) {
    this.schemaName = schemaName;
    return this;
  }

  public SchemaVersionBuilder setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public SchemaVersionBuilder versionOf(ISchema schema) {
    this.catName = schema.getCatName();
    this.dbName = schema.getDbName();
    this.schemaName = schema.getName();
    return this;
  }

  public SchemaVersionBuilder setVersion(int version) {
    this.version = version;
    return this;
  }

  public SchemaVersionBuilder setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
    return this;
  }

  public SchemaVersionBuilder setState(
      SchemaVersionState state) {
    this.state = state;
    return this;
  }

  public SchemaVersionBuilder setDescription(String description) {
    this.description = description;
    return this;
  }

  public SchemaVersionBuilder setSchemaText(String schemaText) {
    this.schemaText = schemaText;
    return this;
  }

  public SchemaVersionBuilder setFingerprint(String fingerprint) {
    this.fingerprint = fingerprint;
    return this;
  }

  public SchemaVersionBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public SchemaVersion build() throws MetaException {
    if (schemaName == null || version < 0) {
      throw new MetaException("You must provide the schema name, and schema version");
    }
    SchemaVersion schemaVersion =
        new SchemaVersion(new ISchemaName(catName, dbName, schemaName), version, createdAt, getCols());
    if (state != null) schemaVersion.setState(state);
    if (description != null) schemaVersion.setDescription(description);
    if (schemaText != null) schemaVersion.setSchemaText(schemaText);
    if (fingerprint != null) schemaVersion.setFingerprint(fingerprint);
    if (name != null) schemaVersion.setName(name);
    schemaVersion.setSerDe(buildSerde());
    return schemaVersion;
  }
}
