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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * abstractExternalDB is incharge of connect to and populate externaal database for qtest
 */
public abstract class AbstractExternalDB {
    private static final Logger LOG = LoggerFactory.getLogger("AbstractExternalDB");

    protected static final String userName = "qtestuser";
    protected static final String password = "qtestpassword";
    protected static final String dbName = "qtestDB";

    public String externalDBType = "derby"; // default: derby
    protected String url = String.format("jdbc:derby:memory:%s;create=true", dbName); // defualt: dervy
    protected String driver = "org.apache.derby.jdbc.EmbeddedDriver"; // default: derby

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