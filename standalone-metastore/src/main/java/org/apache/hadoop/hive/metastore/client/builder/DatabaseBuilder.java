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

import org.apache.hadoop.hive.metastore.api.Database;
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
  private String name, description, location;
  private Map<String, String> params = new HashMap<>();
  private String ownerName;
  private PrincipalType ownerType;

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

  public Database build() throws TException {
    if (name == null) throw new MetaException("You must name the database");
    Database db = new Database(name, description, location, params);
    try {
      if (ownerName != null) ownerName = SecurityUtils.getUser();
      db.setOwnerName(ownerName);
      if (ownerType == null) ownerType = PrincipalType.USER;
      db.setOwnerType(ownerType);
      return db;
    } catch (IOException e) {
      throw MetaStoreUtils.newMetaException(e);
    }
  }
}
