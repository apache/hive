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
package org.apache.hive.testutils.database;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sqlline.SqlLine;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractDatabase {

  private final Logger LOG = LoggerFactory.getLogger(AbstractDatabase.class);

  private final int MAX_STARTUP_WAIT = 5 * 60 * 1000;
  private final String HIVE_USER = "hiveuser";
  private final String HIVE_PASSWORD = "hivepassword";

  private String db = "hivedb";
  protected boolean useDockerDatabaseArg = false;

  public AbstractDatabase setUseDockerDatabaseArg(boolean useDockerDatabaseArg) {
    this.useDockerDatabaseArg = useDockerDatabaseArg;
    return this;
  }

  public AbstractDatabase setDb(String db) {
    this.db = db;
    return this;
  }

  protected abstract String getDockerImageName();
  protected abstract List<String> getDockerBaseArgs();
  public abstract String getDbType();
  public abstract String getDbRootUser();
  public abstract String getDbRootPassword();
  public abstract String getJdbcDriver();
  protected abstract String getJdbcUrl(String hostAddress);
  protected abstract boolean isContainerReady(ProcessResults pr);

  public String getJdbcUrl() {
    return getJdbcUrl(getContainerHostAddress());
  }

  protected String getDockerDatabaseArg() {
    return null;
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

  public String getHiveUser(){
    return HIVE_USER;
  }

  public String getHivePassword(){
    return HIVE_PASSWORD;
  }

  public String getDb() {
    return db;
  }

  public String getDockerContainerName() {
    return String.format("testDb-%s", getClass().getSimpleName());
  }

  private List<String> getDockerAdditionalArgs() {
    List<String> dockerArgs = new ArrayList(getDockerBaseArgs());
    if (useDockerDatabaseArg && StringUtils.isNotEmpty(getDockerDatabaseArg())) {
      dockerArgs.addAll(Arrays.asList("-e", getDockerDatabaseArg()));
    }
    return dockerArgs;
  }

  private String[] buildRunCmd() {
    List<String> cmd =  new ArrayList(
        Arrays.asList("docker", "run", "--rm", "--name"));
    cmd.add(getDockerContainerName());
    cmd.addAll(getDockerAdditionalArgs());
    cmd.add(getDockerImageName());
    return cmd.toArray(new String[cmd.size()]);
  }

  private String getContainerHostAddress() {
    String hostAddress = System.getenv("HIVE_TEST_DOCKER_HOST");
    return hostAddress != null ? hostAddress : "localhost";
  }

  private String[] buildRmCmd() {
    return new String[] { "docker", "rm", "-f", "-v", getDockerContainerName() };
  }

  private String[] buildLogCmd() {
    return new String[] { "docker", "logs", getDockerContainerName() };
  }

  private int runCmdAndPrintStreams(String[] cmd, long secondsToWait)
      throws InterruptedException, IOException {
    ProcessResults results = runCmd(cmd, secondsToWait);
    LOG.info("Stdout from proc: " + results.stdout);
    LOG.info("Stderr from proc: " + results.stderr);
    return results.rc;
  }

  private ProcessResults runCmd(String[] cmd, long secondsToWait)
      throws IOException, InterruptedException {
    LOG.info("Going to run: " + StringUtils.join(cmd, " "));
    Process proc = Runtime.getRuntime().exec(cmd);
    if (!proc.waitFor(secondsToWait, TimeUnit.SECONDS)) {
      throw new RuntimeException(
          "Process " + cmd[0] + " failed to run in " + secondsToWait + " seconds");
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    final StringBuilder lines = new StringBuilder();
    reader.lines().forEach(s -> lines.append(s).append('\n'));

    reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
    final StringBuilder errLines = new StringBuilder();
    reader.lines().forEach(s -> errLines.append(s).append('\n'));
    LOG.info("Result size: " + lines.length() + ";" + errLines.length());
    return new ProcessResults(lines.toString(), errLines.toString(), proc.exitValue());
  }

  public void launchDockerContainer() throws Exception {
    runCmdAndPrintStreams(buildRmCmd(), 600);
    if (runCmdAndPrintStreams(buildRunCmd(), 600) != 0) {
      throw new RuntimeException("Unable to start docker container");
    }
    long startTime = System.currentTimeMillis();
    ProcessResults pr;
    do {
      Thread.sleep(1000);
      pr = runCmd(buildLogCmd(), 30);
      if (pr.rc != 0) {
        throw new RuntimeException("Failed to get docker logs");
      }
    } while (startTime + MAX_STARTUP_WAIT >= System.currentTimeMillis() && !isContainerReady(pr));
    if (startTime + MAX_STARTUP_WAIT < System.currentTimeMillis()) {
      throw new RuntimeException("Container failed to be ready in " + MAX_STARTUP_WAIT/1000 +
          " seconds");
    }
  }

  public void cleanupDockerContainer() throws IOException, InterruptedException {
    if (runCmdAndPrintStreams(buildRmCmd(), 600) != 0) {
      throw new RuntimeException("Unable to remove docker container");
    }
  }

  private String[] SQLLineCmdBuild(String sqlScriptFile) {
    return new String[] {"-u", getJdbcUrl(),
        "-d", getJdbcDriver(),
        "-n", getDbRootUser(),
        "-p", getDbRootPassword(),
        "--isolation=TRANSACTION_READ_COMMITTED",
        "-f", sqlScriptFile};
  }

  public void execute(String script) throws IOException, SQLException, ClassNotFoundException {
    // Test we can connect to database
    Class.forName(getJdbcDriver());
    try (Connection ignored = DriverManager.getConnection(getJdbcUrl(), getDbRootUser(), getDbRootPassword())) {
      LOG.info("Successfully connected to {} with user {} and password {}", getJdbcUrl(), getDbRootUser(), getDbRootPassword());
    }
    LOG.info("Starting {} initialization", getClass().getSimpleName());
    SqlLine sqlLine = new SqlLine();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    sqlLine.setOutputStream(new PrintStream(out));
    sqlLine.setErrorStream(new PrintStream(out));
    System.setProperty("sqlline.silent", "true");
    SqlLine.Status status = sqlLine.begin(SQLLineCmdBuild(script), null, false);
    LOG.debug("Printing output from SQLLine:");
    LOG.debug(out.toString());
    if (status != SqlLine.Status.OK) {
      throw new RuntimeException("Database script " + script + " failed with status " + status);
    }
    LOG.info("Completed {} initialization", getClass().getSimpleName());
  }

  protected class ProcessResults {
    final public String stdout;
    final public String stderr;
    final public int rc;

    public ProcessResults(String stdout, String stderr, int rc) {
      this.stdout = stdout;
      this.stderr = stderr;
      this.rc = rc;
    }
  }
}
