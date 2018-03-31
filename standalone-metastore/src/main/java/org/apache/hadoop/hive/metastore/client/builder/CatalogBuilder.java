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

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

public class CatalogBuilder {
  private String name, description, location;

  public CatalogBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public CatalogBuilder setDescription(String description) {
    this.description = description;
    return this;
  }

  public CatalogBuilder setLocation(String location) {
    this.location = location;
    return this;
  }

  public Catalog build() throws MetaException {
    if (name == null) throw new MetaException("You must name the catalog");
    if (location == null) throw new MetaException("You must give the catalog a location");
    Catalog catalog = new Catalog(name, location);
    if (description != null) catalog.setDescription(description);
    return catalog;
  }

  /**
   * Build the catalog object and create it in the metastore.
   * @param client metastore client
   * @return new catalog object
   * @throws TException thrown from the client
   */
  public Catalog create(IMetaStoreClient client) throws TException {
    Catalog cat = build();
    client.createCatalog(cat);
    return cat;
  }
}
