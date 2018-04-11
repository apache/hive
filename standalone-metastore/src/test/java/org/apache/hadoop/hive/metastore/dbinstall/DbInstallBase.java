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
package org.apache.hadoop.hive.metastore.dbinstall;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.MetastoreSchemaTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class DbInstallBase {

  private static final Logger LOG = LoggerFactory.getLogger(DbInstallBase.class);

  private static final String HIVE_USER = "hiveuser";
  protected static final String HIVE_DB = "hivedb";
  private static final String FIRST_VERSION = "1.2.0";
  private static final int MAX_STARTUP_WAIT = 5 * 60 * 1000;

  private String metastoreHome;

  protected abstract String getDockerContainerName();
  protected abstract String getDockerImageName();
  protected abstract String[] getDockerAdditionalArgs();
  protected abstract String getDbType();
  protected abstract String getDbRootUser();
  protected abstract String getDbRootPassword();
  protected abstract String getJdbcDriver();
  protected abstract String getJdbcUrl();
  /**
   * URL to use when connecting as root rather than Hive
   * @return URL
   */
  protected abstract String getInitialJdbcUrl();

  /**
   * Determine if the docker container is ready to use.
   * @param logOutput output of docker logs command
   * @return true if ready, false otherwise
   */
  protected abstract boolean isContainerReady(String logOutput);
  protected abstract String getHivePassword();

  @Before
  public void runDockerContainer() throws IOException, InterruptedException {
    if (runCmdAndPrintStreams(buildRunCmd(), 600) != 0) {
      throw new RuntimeException("Unable to start docker container");
    }
    long startTime = System.currentTimeMillis();
    ProcessResults pr;
    do {
      Thread.sleep(5000);
      pr = runCmd(buildLogCmd(), 5);
      if (pr.rc != 0) throw new RuntimeException("Failed to get docker logs");
    } while (startTime + MAX_STARTUP_WAIT >= System.currentTimeMillis() && !isContainerReady(pr.stdout));
    if (startTime + MAX_STARTUP_WAIT < System.currentTimeMillis()) {
      throw new RuntimeException("Container failed to be ready in " + MAX_STARTUP_WAIT/1000 +
          " seconds");
    }
    MetastoreSchemaTool.homeDir = metastoreHome = System.getProperty("test.tmp.dir", "target/tmp");
  }

  @After
  public void stopAndRmDockerContainer() throws IOException, InterruptedException {
    if ("true".equalsIgnoreCase(System.getProperty("metastore.itest.no.stop.container"))) {
      LOG.warn("Not stopping container " + getDockerContainerName() + " at user request, please " +
          "be sure to shut it down before rerunning the test.");
      return;
    }
    if (runCmdAndPrintStreams(buildStopCmd(), 60) != 0) {
      throw new RuntimeException("Unable to stop docker container");
    }
    if (runCmdAndPrintStreams(buildRmCmd(), 15) != 0) {
      throw new RuntimeException("Unable to remove docker container");
    }
  }

  private static class ProcessResults {
    final String stdout;
    final String stderr;
    final int rc;

    public ProcessResults(String stdout, String stderr, int rc) {
      this.stdout = stdout;
      this.stderr = stderr;
      this.rc = rc;
    }
  }

  private ProcessResults runCmd(String[] cmd, long secondsToWait) throws IOException,
      InterruptedException {
    LOG.info("Going to run: " + StringUtils.join(cmd, " "));
    Process proc = Runtime.getRuntime().exec(cmd);
    if (!proc.waitFor(secondsToWait, TimeUnit.SECONDS)) {
      throw new RuntimeException("Process " + cmd[0] + " failed to run in " + secondsToWait +
          " seconds");
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    final StringBuilder lines = new StringBuilder();
    reader.lines()
        .forEach(s -> lines.append(s).append('\n'));

    reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
    final StringBuilder errLines = new StringBuilder();
    reader.lines()
        .forEach(s -> errLines.append(s).append('\n'));
    return new ProcessResults(lines.toString(), errLines.toString(), proc.exitValue());
  }

  private int runCmdAndPrintStreams(String[] cmd, long secondsToWait)
      throws InterruptedException, IOException {
    ProcessResults results = runCmd(cmd, secondsToWait);
    LOG.info("Stdout from proc: " + results.stdout);
    LOG.info("Stderr from proc: " + results.stderr);
    return results.rc;
  }

  private int createUser() {
    return MetastoreSchemaTool.run(buildArray(
        "-createUser",
        "-dbType",
        getDbType(),
        "-userName",
        getDbRootUser(),
        "-passWord",
        getDbRootPassword(),
        "-hiveUser",
        HIVE_USER,
        "-hivePassword",
        getHivePassword(),
        "-hiveDb",
        HIVE_DB,
        "-url",
        getInitialJdbcUrl(),
        "-driver",
        getJdbcDriver()
    ));
  }

  private int installLatest() {
    return MetastoreSchemaTool.run(buildArray(
        "-initSchema",
        "-dbType",
        getDbType(),
        "-userName",
        HIVE_USER,
        "-passWord",
        getHivePassword(),
        "-url",
        getJdbcUrl(),
        "-driver",
        getJdbcDriver()
    ));
  }

  private int installAVersion(String version) {
    return MetastoreSchemaTool.run(buildArray(
        "-initSchemaTo",
        version,
        "-dbType",
        getDbType(),
        "-userName",
        HIVE_USER,
        "-passWord",
        getHivePassword(),
        "-url",
        getJdbcUrl(),
        "-driver",
        getJdbcDriver()
    ));
  }

  private int upgradeToLatest() {
    return MetastoreSchemaTool.run(buildArray(
        "-upgradeSchema",
        "-dbType",
        getDbType(),
        "-userName",
        HIVE_USER,
        "-passWord",
        getHivePassword(),
        "-url",
        getJdbcUrl(),
        "-driver",
        getJdbcDriver()
    ));
  }

  protected String[] buildArray(String... strs) {
    return strs;
  }

  private String getCurrentVersionMinusOne() throws HiveMetaException {
    List<String> scripts = MetaStoreSchemaInfoFactory.get(
        MetastoreConf.newMetastoreConf(), metastoreHome, getDbType()
    ).getUpgradeScripts(FIRST_VERSION);
    Assert.assertTrue(scripts.size() > 0);
    String lastUpgradePath = scripts.get(scripts.size() - 1);
    String version = lastUpgradePath.split("-")[1];
    LOG.info("Current version minus 1 is " + version);
    return version;
  }

  @Test
  public void install() {
    Assert.assertEquals(0, createUser());
    Assert.assertEquals(0, installLatest());
  }

  @Test
  public void upgrade() throws HiveMetaException {
    Assert.assertEquals(0, createUser());
    Assert.assertEquals(0, installAVersion(FIRST_VERSION));
    Assert.assertEquals(0, upgradeToLatest());
  }

  private String[] buildRunCmd() {
    List<String> cmd = new ArrayList<>(4 + getDockerAdditionalArgs().length);
    cmd.add("docker");
    cmd.add("run");
    cmd.add("--name");
    cmd.add(getDockerContainerName());
    cmd.addAll(Arrays.asList(getDockerAdditionalArgs()));
    cmd.add(getDockerImageName());
    return cmd.toArray(new String[cmd.size()]);
  }

  private String[] buildStopCmd() {
    return buildArray(
        "docker",
        "stop",
        getDockerContainerName()
    );
  }

  private String[] buildRmCmd() {
    return buildArray(
        "docker",
        "rm",
        getDockerContainerName()
    );
  }

  private String[] buildLogCmd() {
    return buildArray(
        "docker",
        "logs",
        getDockerContainerName()
    );
  }
}
