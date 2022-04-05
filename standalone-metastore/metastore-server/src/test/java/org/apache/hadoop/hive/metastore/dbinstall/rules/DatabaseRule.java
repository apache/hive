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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.ProcessResults;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.buildLogCmd;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.buildRmCmd;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.buildRunCmd;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.runCmd;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.runCmdAndPrintStreams;

/**
 * Abstract JUnit TestRule for different RDMBS types.
 */
public abstract class DatabaseRule extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseRule.class);

  protected static final String HIVE_USER = "hiveuser";
  // used in most of the RDBMS configs, except MSSQL
  protected static final String HIVE_PASSWORD = "hivepassword";
  protected static final String HIVE_DB = "hivedb";
  private static final int MAX_STARTUP_WAIT = 5 * 60 * 1000;

  public abstract String getHivePassword();

  public abstract String getDockerImageName();

  public abstract String[] getDockerAdditionalArgs();

  public abstract String getDbType();

  public abstract String getDbRootUser();

  public abstract String getDbRootPassword();

  public abstract String getJdbcDriver();

  public abstract String getJdbcUrl(String hostAddress);

  public final String getJdbcUrl() {
    return getJdbcUrl(getContainerHostAddress());
  }

  public final String getContainerHostAddress() {
    String hostAddress = System.getenv("HIVE_TEST_DOCKER_HOST");
    if (hostAddress != null) {
      return hostAddress;
    } else {
      return "localhost";
    }
  }

  private boolean verbose;

  public DatabaseRule() {
    verbose = System.getProperty("verbose.schematool") != null;
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
  public abstract String getInitialJdbcUrl(String hostAddress);

  public final String getInitialJdbcUrl() {
    return getInitialJdbcUrl(getContainerHostAddress());
  }

  /**
   * Determine if the docker container is ready to use.
   *
   * @param pr output of docker logs command
   * @return true if ready, false otherwise
   */
  public abstract boolean isContainerReady(ProcessResults pr);

  protected String[] buildArray(String... strs) {
    return strs;
  }

  @Override
  public void before() throws Exception { //runDockerContainer
    runCmdAndPrintStreams(buildRmCmd(getDockerContainerName()), 600);

    final String[] runCmd = buildRunCmd(
        getDockerContainerName(), getDockerAdditionalArgs(), getDockerImageName());
    if (runCmdAndPrintStreams(runCmd, 600) != 0) {
      throw new RuntimeException("Unable to start docker container");
    }
    long startTime = System.currentTimeMillis();
    ProcessResults pr;
    do {
      Thread.sleep(1000);
      pr = runCmd(buildLogCmd(getDockerContainerName()), 30);
      if (pr.getReturnCode() != 0) {
        throw new RuntimeException("Failed to get docker logs");
      }
    } while (startTime + MAX_STARTUP_WAIT >= System.currentTimeMillis() && !isContainerReady(pr));
    if (startTime + MAX_STARTUP_WAIT < System.currentTimeMillis()) {
      throw new RuntimeException("Container failed to be ready in " + MAX_STARTUP_WAIT/1000 +
          " seconds");
    }
    MetastoreSchemaTool.setHomeDirForTesting();
  }

  @Override
  public void after() { // stopAndRmDockerContainer
    if ("true".equalsIgnoreCase(System.getProperty("metastore.itest.no.stop.container"))) {
      LOG.warn("Not stopping container " + getDockerContainerName() + " at user request, please "
          + "be sure to shut it down before rerunning the test.");
      return;
    }
    try {
      if (runCmdAndPrintStreams(buildRmCmd(getDockerContainerName()), 600) != 0) {
        throw new RuntimeException("Unable to remove docker container");
      }
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  protected String getDockerContainerName() {
    String suffix = System.getenv("HIVE_TEST_DOCKER_CONTAINER_SUFFIX");
    if (suffix == null) {
      suffix = "";
    } else {
      suffix = "-" + suffix;
    }
    return String.format("metastore-test-%s-install%s", getDbType(), suffix);
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
