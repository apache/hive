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
package org.apache.hadoop.hive.ql.externalDB;

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

/**
 * The class is in charge of connecting and populating dockerized databases for qtest.
 */
public abstract class AbstractExternalDB {
    protected static final Logger LOG = LoggerFactory.getLogger("AbstractExternalDB");

    protected static final String dbName = "qtestDB";

    private static final int MAX_STARTUP_WAIT = 5 * 60 * 1000;

    protected static class ProcessResults {
        final String stdout;
        final String stderr;
        final int rc;

        public ProcessResults(String stdout, String stderr, int rc) {
            this.stdout = stdout;
            this.stderr = stderr;
            this.rc = rc;
        }
    }

    private final String getDockerContainerName() {
        return String.format("qtestExternalDB-%s", getClass().getSimpleName());
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
        return new String[] { "docker", "rm", "-f", "-v", getDockerContainerName() };
    }

    private String[] buildLogCmd() {
        return new String[] { "docker", "logs", getDockerContainerName() };
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
        return new ProcessResults(lines.toString(), errLines.toString(), proc.exitValue());
    }

    private int runCmdAndPrintStreams(String[] cmd, long secondsToWait)
            throws InterruptedException, IOException {
        ProcessResults results = runCmd(cmd, secondsToWait);
        LOG.info("Stdout from proc: " + results.stdout);
        LOG.info("Stderr from proc: " + results.stderr);
        return results.rc;
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
            pr = runCmd(buildLogCmd(), 5);
            if (pr.rc != 0) {
                throw new RuntimeException("Failed to get docker logs");
            }
        } while (startTime + MAX_STARTUP_WAIT >= System.currentTimeMillis() && !isContainerReady(pr));
        if (startTime + MAX_STARTUP_WAIT < System.currentTimeMillis()) {
            throw new RuntimeException("Container failed to be ready in " + MAX_STARTUP_WAIT/1000 +
                    " seconds");
        }
    }

    public void cleanupDockerContainer() {
        try {
            if (runCmdAndPrintStreams(buildRmCmd(), 600) != 0) {
                LOG.info("Unable to remove docker container");
                throw new RuntimeException("Unable to remove docker container");
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }


    protected final String getContainerHostAddress() {
        String hostAddress = System.getenv("HIVE_TEST_DOCKER_HOST");
        if (hostAddress != null) {
            return hostAddress;
        } else {
            return "localhost";
        }
    }

    public String getUser() {
        return "qtestuser";
    }
    
    public String getPassword() {
        return  "qtestpassword";
    }
    
    public abstract String getJdbcUrl();

    public abstract String getJdbcDriver();

    protected abstract String getDockerImageName();

    protected abstract String[] getDockerAdditionalArgs();

    public abstract boolean isContainerReady(ProcessResults pr);

    protected String[] SQLLineCmdBuild(String sqlScriptFile) {
        return new String[] {"-u", getJdbcUrl(),
                            "-d", getJdbcDriver(),
                            "-n", getUser(),
                            "-p", getPassword(),
                            "--isolation=TRANSACTION_READ_COMMITTED",
                            "-f", sqlScriptFile};

    }

    protected void execSql(String sqlScriptFile) throws IOException {
        SqlLine sqlLine = new SqlLine();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        sqlLine.setOutputStream(new PrintStream(out));
        sqlLine.setErrorStream(new PrintStream(out));
        System.setProperty("sqlline.silent", "true");
        SqlLine.Status status = sqlLine.begin(SQLLineCmdBuild(sqlScriptFile), null, false);
        LOG.debug("Printing output from SQLLine:");
        LOG.debug(out.toString());
        if (status != SqlLine.Status.OK) {
            throw new IOException("external database script failed, errorcode " + status);
        }
    }

    public void execute(String script) throws IOException, SQLException, ClassNotFoundException {
        // Test we can connect to database
        Class.forName(getJdbcDriver());
        try (Connection ignored = DriverManager.getConnection(getJdbcUrl(), getUser(), getPassword())) {
            LOG.info("Successfully connected to {} with user {} and password {}", getJdbcUrl(), getUser(), getPassword());
        }
        LOG.info("Starting {} initialization", getClass().getSimpleName());
        execSql(script);
        LOG.info("Completed {} initialization", getClass().getSimpleName());
    }
}