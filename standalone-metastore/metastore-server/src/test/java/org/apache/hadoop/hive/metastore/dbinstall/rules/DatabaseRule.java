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
import org.junit.rules.ExternalResource;

/**
 * Abstract JUnit TestRule for different RDMBS types.
 */
public abstract class DatabaseRule extends ExternalResource {
  protected static final String HIVE_USER = "hiveuser";
  // used in most of the RDBMS configs, except MSSQL
  protected static final String HIVE_PASSWORD = "hivepassword";
  protected static final String HIVE_DB = "hivedb";

  public abstract String getHivePassword();

  public abstract String getDbType();

  public abstract String getDbRootUser();

  public abstract String getDbRootPassword();

  public abstract String getJdbcDriver();

  public abstract String getJdbcUrl();

  private boolean verbose;

  public DatabaseRule() {
    verbose = System.getProperty("verbose.schematool") != null;
    MetastoreSchemaTool.setHomeDirForTesting();
  }

  public DatabaseRule setVerbose(boolean verbose) {
    this.verbose = verbose;
    return this;
  }

  public String getDb() {
    return HIVE_DB;
  }

  /**
   * URL to use when connecting as root rather than Hive
   *
   * @return URL
   */
  public abstract String getInitialJdbcUrl();

  @Override
  public abstract void before();

  @Override
  public abstract void after();

  protected String[] buildArray(String... strs) {
    return strs;
  }

  public String getHiveUser(){
    return HIVE_USER;
  }

  public int createUser() {
    return new MetastoreSchemaTool().setVerbose(verbose).run(buildArray(
        "-createUser",
        "-dbType",
        getDbType(),
        "-userName",
        getDbRootUser(),
        "-passWord",
        getDbRootPassword(),
        "-hiveUser",
        getHiveUser(),
        "-hivePassword",
        getHivePassword(),
        "-hiveDb",
        getDb(),
        "-url",
        getInitialJdbcUrl(),
        "-driver",
        getJdbcDriver()
    ));
  }

  public int installLatest() {
    return new MetastoreSchemaTool().setVerbose(verbose).run(buildArray(
        "-initSchema",
        "-dbType",
        getDbType(),
        "-userName",
        getHiveUser(),
        "-passWord",
        getHivePassword(),
        "-url",
        getJdbcUrl(),
        "-driver",
        getJdbcDriver(),
        "-verbose"
    ));
  }

  public int installAVersion(String version) {
    return new MetastoreSchemaTool().setVerbose(verbose).run(buildArray(
        "-initSchemaTo",
        version,
        "-dbType",
        getDbType(),
        "-userName",
        getHiveUser(),
        "-passWord",
        getHivePassword(),
        "-url",
        getJdbcUrl(),
        "-driver",
        getJdbcDriver()
    ));
  }

  public int upgradeToLatest() {
    return new MetastoreSchemaTool().setVerbose(verbose).run(buildArray(
        "-upgradeSchema",
        "-dbType",
        getDbType(),
        "-userName",
        getHiveUser(),
        "-passWord",
        getHivePassword(),
        "-url",
        getJdbcUrl(),
        "-driver",
        getJdbcDriver()
    ));
  }

  public void install() {
    createUser();
    installLatest();
  }

  public int validateSchema() {
    return new MetastoreSchemaTool().setVerbose(verbose).run(buildArray(
        "-validate",
        "-dbType",
        getDbType(),
        "-userName",
        getHiveUser(),
        "-passWord",
        getHivePassword(),
        "-url",
        getJdbcUrl(),
        "-driver",
        getJdbcDriver()
    ));
  }
}
