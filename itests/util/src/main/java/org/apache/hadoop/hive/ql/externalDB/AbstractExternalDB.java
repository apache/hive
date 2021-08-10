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

import sqlline.SqlLine;

import org.apache.commons.io.output.NullOutputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * abstractExternalDB is incharge of connect to and populate externaal database for qtest
 */
public abstract class AbstractExternalDB {
    protected static final Logger LOG = LoggerFactory.getLogger("AbstractExternalDB");

    protected static final String userName = "qtestuser";
    protected static final String password = "qtestpassword";
    protected static final String dbName = "qtestDB";

    public String externalDBType = "mysql"; // default: mysql
    protected String url = "jdbc:mysql://" + hostAddress + ":3306/" + dbName; // default: mysql
    protected String driver = "org.mariadb.jdbc.Driver"; // default: mysql
    private static final int MAX_STARTUP_WAIT = 5 * 60 * 1000;

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

    public static AbstractExternalDB initalizeExternalDB(String externalDBType) throws IOException {
        AbstractExternalDB abstractExternalDB;
        switch (externalDBType) {
            case "mysql":
                abstractExternalDB = new MySQLExternalDB();
                break;
            case "derby":
                abstractExternalDB = new DerbyExternalDB();
                break;
            case "postgres":
                abstractExternalDB = new PostgresExternalDB();
                break;
            default:
                throw new IOException("unsupported external database type: " + externalDBType);
        }
        return abstractExternalDB;
    }

    public AbstractExternalDB(String externalDBType) {
        this.externalDBType = externalDBType;
    }

    protected String getDockerContainerName() {
        return String.format("qtestExternalDB-%s", externalDBType);
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


    public void launchDockerContainer() throws Exception { //runDockerContainer
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

    public void cleanupDockerContainer() { // stopAndRmDockerContainer
        try {
            if (runCmdAndPrintStreams(buildRmCmd(), 600) != 0) {
                LOG.info("Unable to remove docker container");
                throw new RuntimeException("Unable to remove docker container");
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }


    public final String getContainerHostAddress() {
        String hostAddress = System.getenv("HIVE_TEST_DOCKER_HOST");
        if (hostAddress != null) {
            return hostAddress;
        } else {
            return "localhost";
        }
    }

    public abstract void setJdbcUrl(String hostAddress);

    public abstract void setJdbcDriver();

    public abstract String getDockerImageName();

    public abstract String[] getDockerAdditionalArgs();

    public abstract boolean isContainerReady(ProcessResults pr);

    protected String[] buildArray(String... strs) {
        return strs;
    }

    public Connection getConnectionToExternalDB() throws SQLException, ClassNotFoundException {
        try {
            LOG.info("external database connection URL:\t " + url);
            LOG.info("JDBC Driver :\t " + driver);
            LOG.info("external database connection User:\t " + userName);
            LOG.info("external database connection Password:\t " + password);

            // load required JDBC driver
            Class.forName(driver);

            // Connect using the JDBC URL and user/password
            Connection conn = DriverManager.getConnection(url, userName, password);
            return conn;
        } catch (SQLException e) {
            LOG.error("Failed to connect to external databse", e);
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            LOG.error("Unable to find driver class", e);
            throw new ClassNotFoundException("Unable to find driver class");
        }
    }

    public void testConnectionToExternalDB() throws SQLException, ClassNotFoundException {
        Connection conn = getConnectionToExternalDB();
        try {
            conn.close();
        } catch (SQLException e) {
            LOG.error("Failed to close external database connection", e);
        }
    }

    protected String[] SQLLineCmdBuild(String sqlScriptFile) throws IOException {
        return new String[] {"-u", url,
                            "-d", driver,
                            "-n", userName,
                            "-p", password,
                            "--isolation=TRANSACTION_READ_COMMITTED",
                            "-f", sqlScriptFile};

    }

    protected void execSql(String sqlScriptFile) throws IOException {
        // run the script using SqlLine
        SqlLine sqlLine = new SqlLine();
        ByteArrayOutputStream outputForLog = null;
        OutputStream out;
        if (LOG.isDebugEnabled()) {
            out = outputForLog = new ByteArrayOutputStream();
        } else {
            out = new NullOutputStream();
        }
        sqlLine.setOutputStream(new PrintStream(out));
        System.setProperty("sqlline.silent", "true");

        SqlLine.Status status = sqlLine.begin(SQLLineCmdBuild(sqlScriptFile), null, false);
        if (LOG.isDebugEnabled() && outputForLog != null) {
            LOG.debug("Received following output from Sqlline:");
            LOG.debug(outputForLog.toString("UTF-8"));
        }
        if (status != SqlLine.Status.OK) {
            throw new IOException("external database script failed, errorcode " + status);
        }
    }

    public void execute(String script) throws IOException, SQLException, ClassNotFoundException {
        testConnectionToExternalDB();
        LOG.info("Starting external database initialization to " + this.externalDBType);

        try {
            LOG.info("Initialization script " + script);
            execSql(script);
            LOG.info("Initialization script completed in external database");

        } catch (IOException e) {
            throw new IOException("initialization in external database FAILED!");
        }

    }
}