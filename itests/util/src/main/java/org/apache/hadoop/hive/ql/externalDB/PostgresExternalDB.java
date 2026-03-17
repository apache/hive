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

/**
 * MySQLExternalDB is a extension of abstractExternalDB
 * Designed for MySQL external database connection
 */
public class PostgresExternalDB extends DockerizedDatabase {

    public PostgresExternalDB() {
    }

    public String getJdbcUrl() {
        return "jdbc:postgresql://" + getContainerHostAddress() + ":" + getPort() + "/" + dbName;
    }

    public String getJdbcDriver() {
        return "org.postgresql.Driver";
    }

    @Override
    protected int getPort() {
        return 5432;
    }

    public String getDockerImageName() {
        return "postgres:9.3";
    }

    public String[] getDockerAdditionalArgs() {
        return new String[] { "-p", getPort() + ":5432",
            "-e", "POSTGRES_PASSWORD=" + getRootPassword(),
            "-e", "POSTGRES_USER=" + getRootUser(),
            "-e", "POSTGRES_DB=" + dbName,
            "-d"
        };
    }

    public boolean isContainerReady(ProcessResults pr) {
        return pr.stdout.contains("database system is ready to accept connections") &&
            pr.stderr.contains("database system is ready to accept connections");
    }

}