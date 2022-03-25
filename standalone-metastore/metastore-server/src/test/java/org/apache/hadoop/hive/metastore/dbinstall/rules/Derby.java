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
package org.apache.hadoop.hive.metastore.dbinstall.rules;

import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;

/**
 * JUnit TestRule for Derby.
 */
public class Derby extends DatabaseRule {

  boolean purgeInAfter;

  public Derby() {
    this(false);
  }

  public Derby(boolean purgeInAfter) {
    super();
    this.purgeInAfter = purgeInAfter;
  }

  @Override
  public String getDockerImageName() {
    return null;
  }

  @Override
  public String[] getDockerAdditionalArgs() {
    return null;
  }

  @Override
  public String getDbType() {
    return "derby";
  }

  @Override
  public String getDbRootUser() {
    return "APP";
  }

  @Override
  public String getHiveUser() {
    return "APP";
  }

  @Override
  public String getDbRootPassword() {
    return "mine";
  }

  @Override
  public String getHivePassword() {
    return "mine";
  }

  @Override
  public String getJdbcDriver() {
    return "org.apache.derby.jdbc.EmbeddedDriver";
  }

  @Override
  public String getJdbcUrl(String hostAddress) {

    return String.format("jdbc:derby:memory:%s;create=true", getDb());
  }

  @Override
  public String getInitialJdbcUrl(String hostAddress) {
    return String.format("jdbc:derby:memory:%s;create=true", getDb());
  }

  public String getDb() {
    return MetaStoreServerUtils.JUNIT_DATABASE_PREFIX;
  };

  @Override
  public boolean isContainerReady(ProcessResults pr) {
    return true;
  }

  @Override
  public void before() throws Exception {
    MetastoreSchemaTool.setHomeDirForTesting();
  }

  @Override
  public void after() {
    if(purgeInAfter) {
      try {
        java.sql.DriverManager.getConnection(String.format("jdbc:derby:memory:%s;drop=true", getDb())).close();
      } catch(Exception e) {
        if(!e.getMessage().contains("dropped")) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public int createUser() {
    return 0; // no-op
  }
}
