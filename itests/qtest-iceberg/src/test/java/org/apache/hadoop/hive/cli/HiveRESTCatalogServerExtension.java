/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.extension.RESTCatalogServer;
import org.junit.rules.ExternalResource;

public class HiveRESTCatalogServerExtension extends ExternalResource {
  private final Configuration conf;
  private final RESTCatalogServer restCatalogServer;

  private HiveRESTCatalogServerExtension(AuthType authType, Class<? extends MetaStoreSchemaInfo> schemaInfoClass) {
    this.conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, authType.name());
    restCatalogServer = new RESTCatalogServer();
    if (schemaInfoClass != null) {
      restCatalogServer.setSchemaInfoClass(schemaInfoClass);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  protected void before() throws Throwable {
    restCatalogServer.start(conf);
  }

  @Override
  protected void after() {
    restCatalogServer.stop();
  }

  public String getRestEndpoint() {
    return restCatalogServer.getRestEndpoint();
  }

  public static class Builder {
    private final AuthType authType;
    private Class<? extends MetaStoreSchemaInfo> metaStoreSchemaClass;

    private Builder(AuthType authType) {
      this.authType = authType;
    }
    
    public Builder addMetaStoreSchemaClassName(Class<? extends MetaStoreSchemaInfo> metaStoreSchemaClass) {
      this.metaStoreSchemaClass = metaStoreSchemaClass;
      return this;
    }

    public HiveRESTCatalogServerExtension build() {
      return new HiveRESTCatalogServerExtension(authType, metaStoreSchemaClass);
    }
  }

  public static Builder builder(AuthType authType) {
    return new Builder(authType);
  }
  
  public RESTCatalogServer getRestCatalogServer() {
    return restCatalogServer;
  }
}