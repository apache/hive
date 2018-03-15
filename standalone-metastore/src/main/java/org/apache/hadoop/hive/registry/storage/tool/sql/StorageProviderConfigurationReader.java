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

import java.util.Map;

public class StorageProviderConfigurationReader {
    private static final String STORAGE_PROVIDER_CONFIGURATION = "storageProviderConfiguration";
    private static final String PROPERTIES = "properties";
    private static final String DB_TYPE = "db.type";
    private static final String DB_PROPERTIES = "db.properties";
    private static final String DATA_SOURCE_URL = "dataSource.url";
    private static final String DATA_SOURCE_USER = "dataSource.user";
    private static final String DATA_SOURCE_PASSWORD = "dataSource.password";

    public StorageProviderConfiguration readStorageConfig(Map<String, Object> conf) {
        Map<String, Object> storageConf = (Map<String, Object>) conf.get(
                STORAGE_PROVIDER_CONFIGURATION);
        if (storageConf == null) {
            throw new RuntimeException("No storageProviderConfiguration in config file.");
        }

        Map<String, Object> properties = (Map<String, Object>) storageConf.get(PROPERTIES);
        if (properties == null) {
            throw new RuntimeException("No properties presented to storageProviderConfiguration.");
        }

        String dbType = (String) properties.get(DB_TYPE);
        if (dbType == null) {
            throw new RuntimeException("No db.type presented to properties.");
        }

        Map<String, Object> dbProps = (Map<String, Object>) properties.get(DB_PROPERTIES);

        return readDatabaseProperties(dbProps, DatabaseType.fromValue(dbType));
    }

    private static StorageProviderConfiguration readDatabaseProperties(Map<String, Object> dbProperties, DatabaseType databaseType) {
        String jdbcUrl = (String) dbProperties.get(DATA_SOURCE_URL);
        String user = (String) dbProperties.getOrDefault(DATA_SOURCE_USER, "");
        String password = (String) dbProperties.getOrDefault(DATA_SOURCE_PASSWORD, "");

        return StorageProviderConfiguration.get(jdbcUrl, user, password, databaseType);
    }
}
