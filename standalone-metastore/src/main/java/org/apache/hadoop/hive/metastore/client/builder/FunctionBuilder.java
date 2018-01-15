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

package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for creating Thrift Function objects for tests, and API usage.
 */
public class FunctionBuilder {
  private String dbName = "default";
  private String funcName = null;
  private String className = null;
  private String owner = null;
  private PrincipalType ownerType;
  private int createTime;
  private FunctionType funcType;
  private List<ResourceUri> resourceUris;

  public FunctionBuilder() {
    // Set some reasonable defaults
    ownerType = PrincipalType.USER;
    createTime = (int) (System.currentTimeMillis() / 1000);
    funcType = FunctionType.JAVA;
    resourceUris = new ArrayList<ResourceUri>();
  }

  public FunctionBuilder setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public FunctionBuilder setDbName(Database db) {
    this.dbName = db.getName();
    return this;
  }

  public FunctionBuilder setName(String funcName) {
    this.funcName = funcName;
    return this;
  }

  public FunctionBuilder setClass(String className) {
    this.className = className;
    return this;
  }

  public FunctionBuilder setOwner(String owner) {
    this.owner = owner;
    return this;
  }

  public FunctionBuilder setOwnerType(PrincipalType ownerType) {
    this.ownerType = ownerType;
    return this;
  }

  public FunctionBuilder setCreateTime(int createTime) {
    this.createTime = createTime;
    return this;
  }

  public FunctionBuilder setFunctionType(FunctionType funcType) {
    this.funcType = funcType;
    return this;
  }

  public FunctionBuilder setResourceUris(List<ResourceUri> resourceUris) {
    this.resourceUris = resourceUris;
    return this;
  }

  public FunctionBuilder addResourceUri(ResourceUri resourceUri) {
    this.resourceUris.add(resourceUri);
    return this;
  }

  public Function build() throws MetaException {
    try {
      if (owner != null) {
        owner = SecurityUtils.getUser();
      }
    } catch (IOException e) {
      throw MetaStoreUtils.newMetaException(e);
    }
    return new Function(funcName, dbName, className, owner, ownerType, createTime, funcType,
        resourceUris);
  }
}
