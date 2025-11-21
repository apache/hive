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
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * The class is in charge of managing the lifecycle of databases for qtest.
 *
 * The database should have at least one root user (admin/superuser) able to modify every aspect of the system. The user
 * either exists by default when the database starts or must created right after startup.
 */
public abstract class AbstractExternalDB {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractExternalDB.class);

    /**
     * A connection property that is accessible through system properties when a specific database is running.
     */
    public enum ConnectionProperty {
        JDBC_URL("jdbc.url", AbstractExternalDB::getJdbcUrl),
        JDBC_USERNAME("jdbc.username", AbstractExternalDB::getRootUser),
        JDBC_PASSWORD("jdbc.password", AbstractExternalDB::getRootPassword),
        HOST("host", AbstractExternalDB::getContainerHostAddress),
        PORT("port", db -> String.valueOf(db.getPort()));

        private final String suffix;
        private final Function<AbstractExternalDB, String> valueSupplier;
        ConnectionProperty(String suffix, Function<AbstractExternalDB, String> valueSupplier) {
            this.suffix = suffix;
            this.valueSupplier = valueSupplier;
        }

        private String fullName(String dbName) {
            return "hive.test.database." + dbName + "." + suffix;
        }

        public void set(AbstractExternalDB db) {
            String property = fullName(db.dbName);
            String value = System.getProperty(property);
            if (value == null) {
                System.setProperty(property, valueSupplier.apply(db));
            } else {
                throw new IllegalStateException("Connection property " + property + " is already set.");
            }
        }

        public void clear(AbstractExternalDB db) {
            System.clearProperty(fullName(db.dbName));
        }
    }

    protected String dbName = "qtestDB";
    private Path initScript;

    protected final String getContainerHostAddress() {
        String hostAddress = System.getenv("HIVE_TEST_DOCKER_HOST");
        if (hostAddress != null) {
            return hostAddress;
        } else {
            return "localhost";
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

    protected abstract int getPort();

    private String[] SQLLineCmdBuild(String sqlScriptFile) {
        return new String[] {"-u", getJdbcUrl(),
                            "-d", getJdbcDriver(),
                            "-n", getRootUser(),
                            "-p", getRootPassword(),
                            "--isolation=TRANSACTION_READ_COMMITTED",
                            "-f", sqlScriptFile};

    }

    /**
     * Sets the name of the database. Must not be called after the database has been started.
     * @param name the name of the database
     */
    public void setName(String name) {
        this.dbName = Objects.requireNonNull(name);
    }

    /**
     * Sets the path to the initialization script.
     * @param initScript the path to the initialization script
     * @throws IllegalArgumentException if the script does not exist on the filesystem
     */
    public void setInitScript(Path initScript) {
        if (Files.exists(initScript)) {
            this.initScript = initScript;
        } else {
            throw new IllegalArgumentException("Initialization script does not exist: " + initScript);
        }
    }

    /**
     * Starts the database and performs any initialization required.
     */
    public void start() throws Exception {
        if (initScript != null) {
            execute(initScript.toString());
        }
        Arrays.stream(ConnectionProperty.values()).forEach(p -> p.set(this));
    }

    /**
     * Stops the database and cleans up any resources.
     *
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the thread is interrupted while waiting for the process to finish
     */
    public void stop() throws IOException, InterruptedException {
        Arrays.stream(ConnectionProperty.values()).forEach(p -> p.clear(this));
    }

    public void execute(String script) throws IOException, SQLException, ClassNotFoundException {
        // Test we can connect to database
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
