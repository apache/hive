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
public class MySQLExternalDB extends AbstractExternalDB {

    public MySQLExternalDB() {
        super("mysql");
        setJdbcUrl(getContainerHostAddress());
        setJdbcDriver();
    }

    public void setJdbcUrl(String hostAddress) {
        this.url = "jdbc:mysql://" + hostAddress + ":3306/" + dbName;
    }

    public void setJdbcDriver() {
        this.driver = "org.mariadb.jdbc.Driver";
    }

    public String getDockerImageName() { return "mariadb:5.5"; }

    public String[] getDockerAdditionalArgs() {
        return buildArray("-p", "3306:3306", "-e", "MYSQL_ROOT_PASSWORD=its-a-secret", "-d");
    }

    public boolean isContainerReady(ProcessResults pr) {
        Pattern pat = Pattern.compile("ready for connections");
        Matcher m = pat.matcher(pr.stderr);
        return m.find() && m.find();
    }
}