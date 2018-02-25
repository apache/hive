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


package org.apache.hadoop.hive.registry.storage.tool.shell;

import org.apache.hadoop.hive.registry.storage.tool.sql.StorageProviderConfiguration;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;

import java.nio.charset.StandardCharsets;

public class ShellFlywayFactory {

    private static final String encoding = StandardCharsets.UTF_8.name();
    private static final String metaDataTableName = "SCRIPT_CHANGE_LOG";
    private static final String shellMigrationPrefix = "v";
    private static final String shellMigrationSuffix = ".sh";
    private static final String shellMigrationSeperator = "__";
    private static final boolean validateOnMigrate = true;
    private static final boolean outOfOrder = false;
    private static final boolean baselineOnMigrate = true;
    private static final String baselineVersion = "000";
    private static final boolean cleanOnValidationError = false;


    public static Flyway get(StorageProviderConfiguration conf, String scriptRootPath) {
        Flyway flyway = new Flyway();

        String location = "filesystem:" + scriptRootPath;
        flyway.setEncoding(encoding);
        flyway.setTable(metaDataTableName);
        flyway.setValidateOnMigrate(validateOnMigrate);
        flyway.setOutOfOrder(outOfOrder);
        flyway.setBaselineOnMigrate(baselineOnMigrate);
        flyway.setBaselineVersion(MigrationVersion.fromVersion(baselineVersion));
        flyway.setCleanOnValidationError(cleanOnValidationError);
        flyway.setLocations(location);
        flyway.setResolvers(new ShellMigrationResolver(flyway.getClassLoader(), location, shellMigrationPrefix, shellMigrationSeperator, shellMigrationSuffix));
        flyway.setDataSource(conf.getUrl(), conf.getUser(), conf.getPassword(), null);

        return flyway;
    }

}
