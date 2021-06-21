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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A builder for {@link Database}.  The name of the new database is required.  Everything else
 * selects reasonable defaults.
 */
public class DatabaseBuilder {
  private String name, description, location, managedLocation, catalogName;
  private Map<String, String> params = new HashMap<>();
  private String ownerName;
  private PrincipalType ownerType;
  private int createTime;
  private DatabaseType type;
  private String connectorName, remoteDBName;

  public DatabaseBuilder() {
  }

  public DatabaseBuilder setCatalogName(String catalogName) {
    this.catalogName = catalogName;
    return this;
  }

  public DatabaseBuilder setCatalogName(Catalog catalog) {
    this.catalogName = catalog.getName();
    return this;
  }

  public DatabaseBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public DatabaseBuilder setDescription(String description) {
    this.description = description;
    return this;
  }

  public DatabaseBuilder setLocation(String location) {
    this.location = location;
    return this;
  }

  public DatabaseBuilder setManagedLocation(String location) {
    this.managedLocation = location;
    return this;
  }

  public DatabaseBuilder setParams(Map<String, String> params) {
    this.params = params;
    return this;
  }

  public DatabaseBuilder addParam(String key, String value) {
    params.put(key, value);
    return this;
  }

  public DatabaseBuilder setOwnerName(String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  public DatabaseBuilder setOwnerType(PrincipalType ownerType) {
    this.ownerType = ownerType;
    return this;
  }

  public DatabaseBuilder setCreateTime(int createTime) {
    this.createTime = createTime;
    return this;
  }

  public DatabaseBuilder setType(DatabaseType type) {
    if (type != null)
      this.type = type;
    return this;
  }

  public DatabaseBuilder setConnectorName(String connectorName) {
    this.connectorName = connectorName;
    return this;
  }

  public DatabaseBuilder setRemoteDBName(String remoteDBName) {
    this.remoteDBName = remoteDBName;
    return this;
  }

  public Database build(Configuration conf) throws MetaException {
    if (name == null) throw new MetaException("You must name the database");
    if (catalogName == null) catalogName = MetaStoreUtils.getDefaultCatalog(conf);
    Database db = new Database(name, description, location, params);
    db.setCatalogName(catalogName);
    db.setCreateTime(createTime);
    if (managedLocation != null)
      db.setManagedLocationUri(managedLocation);
    try {
      if (ownerName == null) ownerName = SecurityUtils.getUser();
      db.setOwnerName(ownerName);
      if (ownerType == null) ownerType = PrincipalType.USER;
      db.setOwnerType(ownerType);
      if (type == null) {
        type = DatabaseType.NATIVE;
        if (connectorName != null || remoteDBName != null) {
          throw new MetaException("connector name or remoteDBName cannot be set for database of type NATIVE");
        }
      } else if (type == DatabaseType.REMOTE) {
        if (connectorName == null)
          throw new MetaException("connector name cannot be null for database of type REMOTE");
        db.setConnector_name(connectorName);
        if (remoteDBName != null) {
          db.setRemote_dbname(remoteDBName);
        }
      }
      db.setType(type);
      return db;
    } catch (IOException e) {
      throw MetaStoreUtils.newMetaException(e);
    }
  }

  public Database buildNoModification(Configuration conf) throws MetaException {
    if (name == null) {
      throw new MetaException("You must name the database");
    }
    if (catalogName == null) {
      catalogName = MetaStoreUtils.getDefaultCatalog(conf);
    }
    Database db = new Database(name, description, location, params);
    db.setCatalogName(catalogName);
    db.setCreateTime(createTime);
    if (managedLocation != null)
      db.setManagedLocationUri(managedLocation);
    return db;
  }

  /**
   * Build the database, create it in the metastore, and then return the db object.
   * @param client metastore client
   * @param conf configuration file
   * @return new database object
   * @throws TException comes from {@link #build(Configuration)} or
   * {@link IMetaStoreClient#createDatabase(Database)}.
   */
  public Database create(IMetaStoreClient client, Configuration conf) throws TException {
    Database db = build(conf);
    client.createDatabase(db);
    return db;
  }

  public Database createNoModification(IMetaStoreClient client, Configuration conf) throws TException {
    Database db = buildNoModification(conf);
    client.createDatabase(db);
    return db;
  }
}
