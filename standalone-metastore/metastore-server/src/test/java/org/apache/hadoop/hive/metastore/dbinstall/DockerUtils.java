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
package org.apache.hadoop.hive.metastore.dbinstall;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DockerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DockerUtils.class);

  private DockerUtils() {
    throw new UnsupportedOperationException("Utility class DockerUtils should not be instantiated");
  }

  private static String getDockerCommand() {
    final String osName = System.getProperty("os.name").toLowerCase();
    if (osName.equals("mac os x")) {
      return "/usr/local/bin/docker";
    }
    return "docker";
  }

  public static String[] buildRunCmd(
      String dockerContainerName, String[] dockerAdditionalArgs, String dockerImageName) {
    List<String> cmd = new ArrayList<>(4 + dockerAdditionalArgs.length);
    cmd.add(getDockerCommand());
    cmd.add("run");
    cmd.add("--rm");
    cmd.add("--name");
    cmd.add(dockerContainerName);
    cmd.addAll(Arrays.asList(dockerAdditionalArgs));
    cmd.add(dockerImageName);
    return cmd.toArray(new String[0]);
  }

  public static String[] buildRmCmd(String dockerContainerName) {
    return new String[] { getDockerCommand(), "rm", "-f", "-v", dockerContainerName };
  }

  public static String[] buildLogCmd(String dockerContainerName) {
    return new String[] { getDockerCommand(), "logs", dockerContainerName };
  }

  @SuppressWarnings("SameParameterValue")
  public static int runCmdAndPrintStreams(String[] cmd, long secondsToWait)
      throws InterruptedException, IOException {
    ProcessResults results = runCmd(cmd, secondsToWait);
    LOG.info("Stdout from proc: ", results.stdout);
    LOG.info("Stderr from proc: ", results.stderr);
    return results.returnCode;
  }

  public static ProcessResults runCmd(String[] cmd, long secondsToWait)
      throws IOException, InterruptedException {
    LOG.info("Going to run: {}", String.join(" ", cmd));
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
    LOG.info("Result size: {};{}", lines.length(), errLines.length());
    return new ProcessResults(lines.toString(), errLines.toString(), proc.exitValue());
  }

  public static class ProcessResults {
    final String stdout;
    final String stderr;
    final int returnCode;

    public ProcessResults(String stdout, String stderr, int returnCode) {
      this.stdout = stdout;
      this.stderr = stderr;
      this.returnCode = returnCode;
    }

    public int getReturnCode() {
      return returnCode;
    }

    public String getStdout() {
      return stdout;
    }

    public String getStderr() {
      return stderr;
    }
  }

  public static String getContainerHostAddress() {
    String hostAddress = System.getenv("HIVE_TEST_DOCKER_HOST");
    if (hostAddress != null) {
      return hostAddress;
    } else {
      return "localhost";
    }
  }
}
