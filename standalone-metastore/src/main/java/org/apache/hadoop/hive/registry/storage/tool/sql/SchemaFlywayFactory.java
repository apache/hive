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

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class SchemaFlywayFactory {

    private static final String encoding = StandardCharsets.UTF_8.name();
    private static final String metaDataTableName = "DATABASE_CHANGE_LOG";
    private static final String sqlMigrationPrefix = "v";
    private static final boolean outOfOrder = false;
    private static final boolean baselineOnMigrate = true;
    private static final String baselineVersion = "000";
    private static final boolean cleanOnValidationError = false;


    public static Flyway get(StorageProviderConfiguration conf, String scriptRootPath, boolean validateOnMigrate) {
        Flyway flyway = new Flyway();

        String location = "filesystem:" + scriptRootPath + File.separator + conf.getDbType();
        flyway.setEncoding(encoding);
        flyway.setTable(metaDataTableName);
        flyway.setSqlMigrationPrefix(sqlMigrationPrefix);
        flyway.setValidateOnMigrate(validateOnMigrate);
        flyway.setOutOfOrder(outOfOrder);
        flyway.setBaselineOnMigrate(baselineOnMigrate);
        flyway.setBaselineVersion(MigrationVersion.fromVersion(baselineVersion));
        flyway.setCleanOnValidationError(cleanOnValidationError);
        flyway.setLocations(location);
        flyway.setDataSource(conf.getUrl(), conf.getUser(), conf.getPassword(), null);

        return flyway;
    }

}
