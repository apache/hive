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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sqlline.SqlLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.ProcessResults;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.buildLogCmd;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.buildRmCmd;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.buildRunCmd;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.runCmd;
import static org.apache.hadoop.hive.metastore.dbinstall.DockerUtils.runCmdAndPrintStreams;

/**
 * The class is in charge of connecting and populating dockerized databases for qtest.
 *
 * The database should have at least one root user (admin/superuser) able to modify every aspect of the system. The user
 * either exists by default when the database starts or must created right after startup.
 */
public abstract class AbstractExternalDB {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractExternalDB.class);

    protected static final String DB_NAME = "qtestDB";

    private static final int MAX_STARTUP_WAIT = 5 * 60 * 1000;

    private String getDockerContainerName() {
        return String.format("qtestExternalDB-%s", getClass().getSimpleName());
    }

    public void launchDockerContainer() throws Exception {
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
    }

    public void cleanupDockerContainer() throws IOException, InterruptedException {
        if (runCmdAndPrintStreams(buildRmCmd(getDockerContainerName()), 600) != 0) {
            throw new RuntimeException("Unable to remove docker container");
        }
    }

    /**
     * Return the name of the root user.
     *
     * Override the method if the name of the root user must be different than the default.
     */
    protected String getRootUser() {
        return "qtestuser";
    }

    /**
     * Return the password of the root user.
     *
     * Override the method if the password must be different than the default.
     */
    protected String getRootPassword() {
        return  "qtestpassword";
    }

    protected abstract String getJdbcUrl();

    protected abstract String getJdbcDriver();

    protected abstract String getDockerImageName();

    protected abstract String[] getDockerAdditionalArgs();

    protected abstract boolean isContainerReady(ProcessResults pr);

    private String[] SQLLineCmdBuild(String sqlScriptFile) {
        return new String[] {"-u", getJdbcUrl(),
                            "-d", getJdbcDriver(),
                            "-n", getRootUser(),
                            "-p", getRootPassword(),
                            "--isolation=TRANSACTION_READ_COMMITTED",
                            "-f", sqlScriptFile};
    }

    public void execute(String script) throws IOException, SQLException, ClassNotFoundException {
        // Test we can connect to database
        Class.forName(getJdbcDriver());
        try (Connection ignored = DriverManager.getConnection(getJdbcUrl(), getRootUser(), getRootPassword())) {
            LOG.info("Successfully connected to {} with user {} and password {}", getJdbcUrl(), getRootUser(), getRootPassword());
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
}
