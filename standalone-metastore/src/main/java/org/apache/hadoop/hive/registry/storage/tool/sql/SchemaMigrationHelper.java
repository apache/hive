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

package org.apache.hadoop.hive.registry.storage.tool.sql;

import org.apache.hadoop.hive.registry.storage.tool.sql.exception.SchemaMigrationException;
import org.flywaydb.core.Flyway;
import static org.flywaydb.core.internal.info.MigrationInfoDumper.dumpToAsciiTable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SchemaMigrationHelper {

    private Flyway flyway;

    public SchemaMigrationHelper(Flyway flyway) {
        this.flyway = flyway;
    }

    private void create() throws SQLException {
        try (Connection connection = flyway.getDataSource().getConnection()) {
            if (!isDatabaseEmpty(connection))
                throw new SchemaMigrationException("Please use an empty database or use \"migrate\" if you are already running a previous version.");
        }
        flyway.migrate();
    }

    private void migrate() throws SQLException {
        flyway.migrate();
    }

    private void clean() {
        flyway.clean();
    }

    private void checkConnection() {
        try (Connection connection = flyway.getDataSource().getConnection()) {
            // do nothing
        } catch (Exception e) {
            throw new SchemaMigrationException(e);
        }
    }

    private void info() {
        System.out.println(dumpToAsciiTable(flyway.info().all()));
    }

    private void validate() {
        flyway.validate();
    }

    private void repair() {
        flyway.repair();
    }

    private boolean isDatabaseEmpty(Connection connection) throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        try (ResultSet resultSet = databaseMetaData.getTables(connection.getCatalog(), connection.getSchema(), "", null)) {
            // If the database has any entity like views, tables etc, resultSet.next() would return true here
            return !resultSet.next();
        } catch (SQLException e) {
            throw new SchemaMigrationException("Unable the obtain the state of the target database", e);
        }
    }

    public void execute(SchemaMigrationOption schemaMigrationOption) throws SQLException {
        switch (schemaMigrationOption) {
            case CREATE:
                create();
                break;
            case MIGRATE:
                migrate();
                break;
            case INFO:
                info();
                break;
            case VALIDATE:
                validate();
                break;
            case DROP:
                clean();
                break;
            case CHECK_CONNECTION:
                checkConnection();
                break;
            case REPAIR:
                repair();
                break;
            default:
                throw new SchemaMigrationException("SchemaMigrationHelper unable to execute the option : " + schemaMigrationOption.toString());
        }
    }
}
