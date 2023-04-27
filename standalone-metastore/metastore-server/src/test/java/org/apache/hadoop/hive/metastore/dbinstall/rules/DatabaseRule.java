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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static class ProcessResults {
    final String stdout;
    final String stderr;
    final int rc;

    public ProcessResults(String stdout, String stderr, int rc) {
      this.stdout = stdout;
      this.stderr = stderr;
      this.rc = rc;
    }
  }

  @Override
  public void before() throws Exception { //runDockerContainer
    runCmdAndPrintStreams(buildRmCmd(), 600);
    if (runCmdAndPrintStreams(buildRunCmd(), 600).rc != 0) {
      printDockerEvents();
      throw new RuntimeException("Unable to start docker container");
    }
    long startTime = System.currentTimeMillis();
    ProcessResults pr;
    do {
      Thread.sleep(1000);
      pr = runCmdAndPrintStreams(buildLogCmd(), 30);
      if (pr.rc != 0) {
        printDockerEvents();
        throw new RuntimeException("Failed to get docker logs");
      }
    } while (startTime + MAX_STARTUP_WAIT >= System.currentTimeMillis() && !isContainerReady(pr));
    if (startTime + MAX_STARTUP_WAIT < System.currentTimeMillis()) {
      printDockerEvents();
      throw new RuntimeException(
          String.format("Container initialization failed within %d seconds. Please check the hive logs.",
              MAX_STARTUP_WAIT / 1000));
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
      if (runCmdAndPrintStreams(buildRmCmd(), 600).rc != 0) {
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

  private ProcessResults runCmd(String[] cmd, long secondsToWait)
      throws IOException, InterruptedException {
    LOG.info("Going to run: " + StringUtils.join(cmd, " "));
    Process proc = Runtime.getRuntime().exec(cmd);
    if (!proc.waitFor(Math.abs(secondsToWait), TimeUnit.SECONDS)) {
      throw new RuntimeException("Process " + cmd[0] + " failed to run in " + secondsToWait + " seconds");
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    final StringBuilder lines = new StringBuilder();
    reader.lines().forEach(s -> lines.append(s).append('\n'));

    reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
    final StringBuilder errLines = new StringBuilder();
    reader.lines().forEach(s -> errLines.append(s).append('\n'));
    LOG.info("Result lines#: {}(stdout);{}(stderr)",lines.length(), errLines.length());
    return new ProcessResults(lines.toString(), errLines.toString(), proc.exitValue());
  }

  private ProcessResults runCmdAndPrintStreams(String[] cmd, long secondsToWait)
      throws InterruptedException, IOException {
    ProcessResults results = runCmd(cmd, secondsToWait);
    LOG.info("Stdout from proc: " + results.stdout);
    LOG.info("Stderr from proc: " + results.stderr);
    return results;
  }

  protected void printDockerEvents() {
    try {
      runCmdAndPrintStreams(new String[] { "docker", "events", "--since", "24h", "--until", "0s" }, 3);
    } catch (Exception e) {
      LOG.warn("A problem was encountered while attempting to retrieve Docker events (the system made an analytical"
          + " best effort to list the events to reveal the root cause). No further actions are necessary.", e);
    }
  }

  private String[] buildRunCmd() {
    List<String> cmd = new ArrayList<>(4 + getDockerAdditionalArgs().length);
    cmd.add("docker");
    cmd.add("run");
    cmd.add("--rm");
    cmd.add("--name");
    cmd.add(getDockerContainerName());
    cmd.addAll(Arrays.asList(getDockerAdditionalArgs()));
    cmd.add(getDockerImageName());
    return cmd.toArray(new String[cmd.size()]);
  }

  private String[] buildRmCmd() {
    return buildArray(
        "docker",
        "rm",
        "-f",
        "-v",
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
