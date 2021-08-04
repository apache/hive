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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * MySQLExternalDB is a extension of abstractExternalDB
 * Designed for MySQL external database connection
 */
public class PostgresExternalDB extends AbstractExternalDB {

    public PostgresExternalDB() {
        super("postgres");
        setJdbcUrl(getContainerHostAddress());
        setJdbcDriver();
    }

    public void setJdbcUrl(String hostAddress) {
        this.url = "jdbc:postgresql://" + hostAddress + ":5432/" + dbName;
    }

    public void setJdbcDriver() {
        this.driver = "org.postgresql.Driver";
    }

    public String getDockerImageName() {
        return "postgres:9.3";
    }

    public String[] getDockerAdditionalArgs() {
        return buildArray("-p", "5432:5432", "-e", "POSTGRES_PASSWORD=its-a-secret", "-d");
    }

    public boolean isContainerReady(ProcessResults pr) {
        if (pr.stdout.contains("PostgreSQL init process complete; ready for start up")) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(getContainerHostAddress(), 5432), 1000);
                return true;
            } catch (IOException e) {
                LOG.info("cant connect to postgres; {}", e.getMessage());
                return false;
            }
        }
        return false;
    }

}